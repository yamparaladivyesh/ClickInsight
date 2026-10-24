
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _single_value(dataframe: DataFrame, expression, alias: str):
    row = dataframe.agg(expression.alias(alias)).first()
    return row[alias]


def validate_dashboard_results(
    input_event_count: int,
    sessionized_event_count: int,
    session_count: int,
    results: dict,
) -> bool:
    """
    Validate consistency of CLICKINSIGHT journey outputs before
    they are consumed by the analytical dashboard.
    """

    print("\nDASHBOARD DATA QUALITY VALIDATION")
    print("-" * 72)

    # 1. Input and sessionized event counts must match.
    _require(
        input_event_count == sessionized_event_count,
        "Input and sessionized event counts do not match.",
    )
    print("[PASS] Event counts are consistent.")

    # 2. Journey summary must represent the expected session population.
    summary = results["summary"].first()
    summary_sessions = int(summary["session_count"])
    purchase_sessions = int(summary["purchase_sessions"])
    no_purchase_sessions = int(
        summary["sessions_without_observed_purchase"]
    )

    _require(
        summary_sessions == session_count,
        "Journey summary session count does not match sessionization.",
    )
    _require(
        purchase_sessions + no_purchase_sessions == session_count,
        "Purchase and no-observed-purchase sessions do not reconcile.",
    )
    _require(
        0 <= float(summary["purchase_session_rate"]) <= 1,
        "Purchase-session rate is outside [0, 1].",
    )
    print("[PASS] Overall session and purchase metrics reconcile.")

    # 3. Every session must appear once in the journey-length distribution.
    length_total = int(
        _single_value(
            results["journey_length_distribution"],
            F.sum("session_count"),
            "total",
        )
    )
    _require(
        length_total == session_count,
        "Journey-length distribution does not cover all sessions.",
    )
    print("[PASS] Journey-length distribution covers all sessions.")

    # 4. Entry and exit session totals should each reconcile.
    for key, label in (
        ("entry_events", "Entry-event"),
        ("exit_events", "Exit-event"),
    ):
        total = int(
            _single_value(
                results[key],
                F.sum("session_count"),
                "total",
            )
        )
        _require(
            total == session_count,
            f"{label} session counts do not reconcile.",
        )
        print(f"[PASS] {label} session counts reconcile.")

    # 5. Each segment dimension must account for the full population.
    segments = results["segments"]

    segment_totals = (
        segments.groupBy("segment_type")
        .agg(F.sum("session_count").alias("total_sessions"))
        .collect()
    )

    observed_types = set()

    for row in segment_totals:
        segment_type = row["segment_type"]
        total = int(row["total_sessions"])
        observed_types.add(segment_type)

        _require(
            total == session_count,
            f"Segment '{segment_type}' covers {total} sessions, "
            f"expected {session_count}.",
        )

    _require(
        observed_types == {"device", "source", "country"},
        f"Unexpected segment types: {observed_types}",
    )
    print("[PASS] Device, source, and country segments reconcile.")

    # 6. Segment rates must be valid proportions.
    invalid_segment_rates = segments.filter(
        F.col("purchase_session_rate").isNull()
        | (F.col("purchase_session_rate") < 0)
        | (F.col("purchase_session_rate") > 1)
    ).limit(1).count()

    _require(
        invalid_segment_rates == 0,
        "Invalid purchase-session rate found in segment analysis.",
    )
    print("[PASS] Segment purchase-session rates are valid.")

    # 7. Every represented source event must have outgoing probability 1.
    probability_sums = (
        results["transitions"]
        .groupBy("from_event")
        .agg(
            F.sum(
                F.col("transition_probability").cast("double")
            ).alias("probability_sum")
        )
        .collect()
    )

    _require(
        len(probability_sums) > 0,
        "No Markov transitions were produced.",
    )

    for row in probability_sums:
        probability_sum = row["probability_sum"]

        _require(
            probability_sum is not None
            and abs(float(probability_sum) - 1.0) <= 1e-9,
            f"Invalid outgoing probability sum for {row['from_event']}.",
        )

    print("[PASS] Markov outgoing probabilities sum to 1.")
    print("\nAll dashboard data quality checks passed.")

    return True
