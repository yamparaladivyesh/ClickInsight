
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PURCHASE_EVENT = "purchase"

REQUIRED_COLUMNS = {
    "session_id", "event_id", "event_type", "timestamp",
    "event_sequence", "device", "source", "country",
}


def validate_journey_input(sessionized_events: DataFrame) -> bool:
    missing = REQUIRED_COLUMNS - set(sessionized_events.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    invalid = sessionized_events.filter(
        F.col("session_id").isNull()
        | F.col("event_type").isNull()
        | F.col("event_sequence").isNull()
        | F.col("timestamp").isNull()
    )
    if invalid.limit(1).count():
        raise ValueError("Null session, event, sequence or timestamp found.")

    duplicates = (
        sessionized_events.groupBy("session_id", "event_sequence")
        .count().filter(F.col("count") > 1)
    )
    if duplicates.limit(1).count():
        raise ValueError("Event sequence is not unique within a session.")

    return True



def build_ordered_journeys(sessionized_events: DataFrame) -> DataFrame:
    """Create one ordered journey per session and calculate session metrics."""

    if "customer_id" in sessionized_events.columns:
        customer_expression = F.first(
            "customer_id", ignorenulls=True
        ).alias("customer_id")
    else:
        customer_expression = F.lit(None).cast("string").alias("customer_id")

    grouped = (
        sessionized_events
        .groupBy("session_id")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("event_sequence").alias("sequence"),
                        F.col("event_type").alias("event"),
                        F.col("timestamp").alias("event_timestamp"),
                    )
                )
            ).alias("_ordered_events"),
            customer_expression,
            F.first("device", ignorenulls=True).alias("device"),
            F.first("source", ignorenulls=True).alias("source"),
            F.first("country", ignorenulls=True).alias("country"),
            F.min("timestamp").alias("session_start"),
            F.max("timestamp").alias("session_end"),

            # Safe: returns NULL if the session has no purchase.
            F.min(
                F.when(
                    F.col("event_type") == PURCHASE_EVENT,
                    F.col("timestamp"),
                )
            ).alias("first_purchase_timestamp"),

            F.count("*").alias("event_count"),
            F.countDistinct("event_type").alias("unique_event_types"),
        )
    )

    journeys = (
        grouped
        .withColumn(
            "journey",
            F.transform(
                F.col("_ordered_events"),
                lambda item: item["event"],
            ),
        )
        .withColumn(
            "journey_path",
            F.concat_ws(" -> ", F.col("journey")),
        )
        .withColumn("first_event", F.element_at("journey", 1))
        .withColumn("last_event", F.element_at("journey", -1))
        .withColumn(
            "converted",
            F.array_contains(F.col("journey"), PURCHASE_EVENT),
        )
        .withColumn(
            "journey_outcome",
            F.when(F.col("converted"), "purchase")
            .otherwise("no_purchase_observed"),
        )
        .withColumn(
            "session_duration_minutes",
            (
                F.col("session_end").cast("long")
                - F.col("session_start").cast("long")
            ) / 60.0,
        )
        .withColumn(
            "time_to_first_purchase_minutes",
            F.when(
                F.col("first_purchase_timestamp").isNotNull(),
                (
                    F.col("first_purchase_timestamp").cast("long")
                    - F.col("session_start").cast("long")
                ) / 60.0,
            ),
        )
        .withColumn(
            "repeated_consecutive_event_count",
            F.when(
                F.size("journey") > 1,
                F.expr(
                    "aggregate(sequence(2, size(journey)), 0, "
                    "(acc, i) -> acc + IF("
                    "element_at(journey, i) = "
                    "element_at(journey, i - 1), 1, 0))"
                ),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "has_consecutive_repeat",
            F.col("repeated_consecutive_event_count") > 0,
        )
        .withColumn(
            "repeated_event_type_count",
            F.size("journey") - F.col("unique_event_types"),
        )
        .withColumn(
            "has_repeated_event_type",
            F.col("repeated_event_type_count") > 0,
        )
        .withColumn(
            "event_count_bucket",
            F.when(
                F.col("event_count") <= 5,
                F.format_string("%02d", F.col("event_count")),
            )
            .when(F.col("event_count") <= 10, F.lit("06-10"))
            .when(F.col("event_count") <= 20, F.lit("11-20"))
            .otherwise(F.lit("21+")),
        )
        .drop("_ordered_events")
    )

    return journeys


def build_transition_analysis(journeys: DataFrame) -> DataFrame:
    events = journeys.select(
        "session_id",
        F.posexplode("journey").alias("position", "event_type"),
    )
    a, b = events.alias("a"), events.alias("b")

    pairs = (
        a.join(
            b,
            (F.col("a.session_id") == F.col("b.session_id"))
            & (F.col("b.position") == F.col("a.position") + 1),
            "inner",
        )
        .select(
            F.col("a.session_id").alias("session_id"),
            F.col("a.event_type").alias("from_event"),
            F.col("b.event_type").alias("to_event"),
        )
    )

    counts = pairs.groupBy("from_event", "to_event").agg(
        F.count("*").alias("transition_count"),
        F.countDistinct("session_id").alias("sessions_with_transition"),
    )
    outgoing = counts.groupBy("from_event").agg(
        F.sum("transition_count").alias("outgoing_transition_count")
    )

    return (
        counts.join(outgoing, "from_event")
        .withColumn(
            "transition_probability",
            F.col("transition_count") / F.col("outgoing_transition_count"),
        )
        .orderBy("from_event", "to_event")
    )


def build_common_journeys(journeys: DataFrame) -> DataFrame:
    paths = journeys.groupBy("journey_path").agg(
        F.count("*").alias("session_count"),
        F.sum(F.when(F.col("converted"), 1).otherwise(0))
        .alias("purchase_sessions"),
        F.sum(F.when(~F.col("converted"), 1).otherwise(0))
        .alias("sessions_without_observed_purchase"),
        F.avg("event_count").alias("mean_events_per_session"),
        F.avg("session_duration_minutes")
        .alias("mean_session_duration_minutes"),
    )

    total = paths.agg(F.sum("session_count").alias("total_sessions"))
    return (
        paths.crossJoin(total)
        .withColumn(
            "session_share",
            F.col("session_count") / F.col("total_sessions"),
        )
        .withColumn(
            "purchase_session_rate",
            F.col("purchase_sessions") / F.col("session_count"),
        )
        .drop("total_sessions")
        .orderBy(F.desc("session_count"))
    )


def build_journey_summary(journeys: DataFrame) -> DataFrame:
    summary = journeys.agg(
        F.count("*").alias("session_count"),
        F.sum(F.when(F.col("converted"), 1).otherwise(0))
        .alias("purchase_sessions"),
        F.sum(F.when(~F.col("converted"), 1).otherwise(0))
        .alias("sessions_without_observed_purchase"),
        F.avg("event_count").alias("mean_events_per_session"),
        F.min("event_count").alias("min_events_per_session"),
        F.max("event_count").alias("max_events_per_session"),
        F.expr("percentile_approx(event_count, 0.25, 10000)")
        .alias("p25_events_per_session"),
        F.expr("percentile_approx(event_count, 0.50, 10000)")
        .alias("median_events_per_session"),
        F.expr("percentile_approx(event_count, 0.75, 10000)")
        .alias("p75_events_per_session"),
        F.expr("percentile_approx(event_count, 0.90, 10000)")
        .alias("p90_events_per_session"),
        F.expr("percentile_approx(event_count, 0.95, 10000)")
        .alias("p95_events_per_session"),
        F.avg("session_duration_minutes")
        .alias("mean_session_duration_minutes"),
        F.min("session_duration_minutes")
        .alias("min_session_duration_minutes"),
        F.max("session_duration_minutes")
        .alias("max_session_duration_minutes"),
        F.expr("percentile_approx(session_duration_minutes, 0.50, 10000)")
        .alias("median_session_duration_minutes"),
        F.expr("percentile_approx(session_duration_minutes, 0.90, 10000)")
        .alias("p90_session_duration_minutes"),
        F.avg("time_to_first_purchase_minutes")
        .alias("mean_time_to_first_purchase_minutes"),
        F.expr(
            "percentile_approx(time_to_first_purchase_minutes, 0.50, 10000)"
        ).alias("median_time_to_first_purchase_minutes"),
        F.sum(F.when(F.col("has_consecutive_repeat"), 1).otherwise(0))
        .alias("sessions_with_consecutive_repeats"),
        F.sum(F.when(F.col("has_repeated_event_type"), 1).otherwise(0))
        .alias("sessions_with_repeated_event_types"),
    )
    return summary.withColumn(
        "purchase_session_rate",
        F.col("purchase_sessions") / F.col("session_count"),
    )


def build_journey_length_distribution(journeys: DataFrame) -> DataFrame:
    result = journeys.groupBy("event_count").agg(
        F.count("*").alias("session_count"),
        F.sum(F.when(F.col("converted"), 1).otherwise(0))
        .alias("purchase_sessions"),
        F.sum(F.when(~F.col("converted"), 1).otherwise(0))
        .alias("sessions_without_observed_purchase"),
        F.avg("session_duration_minutes")
        .alias("mean_session_duration_minutes"),
    )
    return (
        result
        .withColumn(
            "purchase_session_rate",
            F.col("purchase_sessions") / F.col("session_count"),
        )
        .orderBy("event_count")
    )


def build_entry_exit_analysis(
    journeys: DataFrame, event_column: str
) -> DataFrame:
    if event_column not in {"first_event", "last_event"}:
        raise ValueError("Use first_event or last_event.")

    counts = journeys.groupBy(event_column).agg(
        F.count("*").alias("session_count"),
        F.sum(F.when(F.col("converted"), 1).otherwise(0))
        .alias("purchase_sessions"),
    )
    total = journeys.agg(F.count("*").alias("total_sessions"))

    return (
        counts.crossJoin(total)
        .withColumn(
            "session_share",
            F.col("session_count") / F.col("total_sessions"),
        )
        .withColumn(
            "purchase_session_rate",
            F.col("purchase_sessions") / F.col("session_count"),
        )
        .withColumnRenamed(event_column, "event_type")
        .drop("total_sessions")
        .orderBy(F.desc("session_count"))
    )


def build_repeated_behavior_analysis(journeys: DataFrame) -> DataFrame:
    return journeys.agg(
        F.count("*").alias("session_count"),
        F.sum(F.when(F.col("has_consecutive_repeat"), 1).otherwise(0))
        .alias("sessions_with_consecutive_repeats"),
        F.avg("repeated_consecutive_event_count")
        .alias("mean_consecutive_repeat_count_per_session"),
        F.sum(F.when(F.col("has_repeated_event_type"), 1).otherwise(0))
        .alias("sessions_with_repeated_event_types"),
        F.avg("repeated_event_type_count")
        .alias("mean_repeated_event_type_count_per_session"),
    )



def build_segment_analysis(journeys: DataFrame) -> DataFrame:
    """
    Summarize session behavior by device, source, and country.

    Uses one aggregation per segment dimension and avoids unioning
    three separately ordered DataFrames.
    """
    frames = []

    for column in ("device", "source", "country"):
        frame = (
            journeys
            .groupBy(column)
            .agg(
                F.count("*").alias("session_count"),
                F.sum(
                    F.when(F.col("converted"), 1).otherwise(0)
                ).alias("purchase_sessions"),
                F.sum(
                    F.when(~F.col("converted"), 1).otherwise(0)
                ).alias("sessions_without_observed_purchase"),
                F.avg("event_count").alias("mean_events_per_session"),
                F.expr(
                    "percentile_approx(event_count, 0.50, 1000)"
                ).alias("median_events_per_session"),
                F.avg(
                    "session_duration_minutes"
                ).alias("mean_session_duration_minutes"),
                F.avg(
                    "time_to_first_purchase_minutes"
                ).alias("mean_time_to_first_purchase_minutes"),
                F.sum(
                    F.when(
                        F.col("has_consecutive_repeat"), 1
                    ).otherwise(0)
                ).alias("sessions_with_consecutive_repeats"),
            )
            .withColumn(
                "purchase_session_rate",
                F.col("purchase_sessions") / F.col("session_count"),
            )
            .withColumn("segment_type", F.lit(column))
            .withColumnRenamed(column, "segment_value")
            .select(
                "segment_type",
                "segment_value",
                "session_count",
                "purchase_sessions",
                "sessions_without_observed_purchase",
                "purchase_session_rate",
                "mean_events_per_session",
                "median_events_per_session",
                "mean_session_duration_minutes",
                "mean_time_to_first_purchase_minutes",
                "sessions_with_consecutive_repeats",
            )
        )

        frames.append(frame)

    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)

    # Sorting is applied by the caller when displaying the results.
    return result



def validate_journey_results(
    journeys: DataFrame,
    transitions: DataFrame,
    summary: DataFrame,
    journey_length_distribution: DataFrame = None,
) -> bool:
    if journeys.filter(
        F.col("journey").isNull() | (F.size("journey") == 0)
    ).limit(1).count():
        raise ValueError("Empty journey found.")

    if journeys.filter(
        F.col("event_count") != F.size("journey")
    ).limit(1).count():
        raise ValueError("Journey length does not match event_count.")

    count = journeys.count()
    row = summary.first()
    if row is None or row["session_count"] != count:
        raise ValueError("Journey summary count is inconsistent.")

    invalid = transitions.filter(
        F.col("transition_probability").isNull()
        | (F.col("transition_probability") < 0)
        | (F.col("transition_probability") > 1)
    )
    if invalid.limit(1).count():
        raise ValueError("Invalid Markov probability.")

    sums = transitions.groupBy("from_event").agg(
        F.sum("transition_probability").alias("probability_sum")
    )
    if sums.filter(
        F.abs(F.col("probability_sum") - 1.0) > 1e-9
    ).limit(1).count():
        raise ValueError("Markov probabilities do not sum to 1.")

    if journey_length_distribution is not None:
        total = journey_length_distribution.agg(
            F.sum("session_count").alias("total")
        ).first()["total"]
        if total != count:
            raise ValueError("Journey-length distribution count mismatch.")

    return True


def run_journey_analysis(sessionized_events: DataFrame) -> dict:
    validate_journey_input(sessionized_events)

    journeys = build_ordered_journeys(sessionized_events)
    transitions = build_transition_analysis(journeys)
    common_journeys = build_common_journeys(journeys)
    summary = build_journey_summary(journeys)
    segments = build_segment_analysis(journeys)
    length_distribution = build_journey_length_distribution(journeys)
    entry_events = build_entry_exit_analysis(journeys, "first_event")
    exit_events = build_entry_exit_analysis(journeys, "last_event")
    repeated_behavior = build_repeated_behavior_analysis(journeys)

    validate_journey_results(
        journeys, transitions, summary, length_distribution
    )

    return {
        "journeys": journeys,
        "transitions": transitions,
        "common_journeys": common_journeys,
        "summary": summary,
        "segments": segments,
        "journey_length_distribution": length_distribution,
        "entry_events": entry_events,
        "exit_events": exit_events,
        "repeated_behavior": repeated_behavior,
        "purchase_journeys": journeys.filter(F.col("converted")),
        "non_purchase_journeys": journeys.filter(~F.col("converted")),
    }
