from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# SESSIONIZATION
# ============================================================

def create_sessionized_events(
    events: DataFrame,
    sessions: DataFrame
) -> DataFrame:
    """
    Create the canonical sessionized event DataFrame.

    Input:
        events   - preprocessed events DataFrame
        sessions - preprocessed sessions DataFrame

    Output:
        sessionized events DataFrame
    """

    # --------------------------------------------------------
    # Select session metadata
    # --------------------------------------------------------

    session_metadata = (
        sessions
        .select(
            "session_id",
            "customer_id",
            "device",
            "source",
            "country"
        )
        .dropDuplicates(["session_id"])
    )

    # --------------------------------------------------------
    # Join event records with session metadata
    # --------------------------------------------------------

    events_with_metadata = (
        events
        .join(
            session_metadata,
            on="session_id",
            how="left"
        )
    )

    # --------------------------------------------------------
    # Canonical chronological ordering
    #
    # timestamp is the primary ordering field.
    # event_id is the deterministic tie-breaker.
    # --------------------------------------------------------

    session_window = (
        Window
        .partitionBy("session_id")
        .orderBy(
            F.col("timestamp").asc(),
            F.col("event_id").asc()
        )
    )

    sessionized_events = (
        events_with_metadata
        .withColumn(
            "event_sequence",
            F.row_number().over(session_window)
        )
    )

    return sessionized_events


# ============================================================
# SESSION SUMMARY
# ============================================================

def create_session_summary(
    sessionized_events: DataFrame
) -> DataFrame:
    """
    Create one row per session.
    """

    session_summary = (
        sessionized_events
        .groupBy("session_id")
        .agg(
            F.first(
                "customer_id",
                ignorenulls=True
            ).alias("customer_id"),

            F.first(
                "device",
                ignorenulls=True
            ).alias("device"),

            F.first(
                "source",
                ignorenulls=True
            ).alias("source"),

            F.first(
                "country",
                ignorenulls=True
            ).alias("country"),

            F.min("timestamp").alias("session_start"),

            F.max("timestamp").alias("session_end"),

            F.count("*").alias("event_count"),

            F.countDistinct(
                "event_type"
            ).alias("unique_event_types")
        )
        .withColumn(
            "session_duration_minutes",
            (
                F.col("session_end").cast("long")
                - F.col("session_start").cast("long")
            ) / 60.0
        )
    )

    return session_summary


# ============================================================
# VALIDATION
# ============================================================

def validate_sessionization(
    original_events: DataFrame,
    sessionized_events: DataFrame,
    session_summary: DataFrame
):
    """
    Validate the canonical sessionization output.

    Raises ValueError when a critical validation fails.
    """

    original_count = original_events.count()

    sessionized_count = sessionized_events.count()

    if original_count != sessionized_count:

        raise ValueError(
            "Sessionization changed the number of events: "
            f"{original_count} -> {sessionized_count}"
        )

    # --------------------------------------------------------
    # Missing session IDs
    # --------------------------------------------------------

    missing_session_id = (
        sessionized_events
        .filter(F.col("session_id").isNull())
        .count()
    )

    if missing_session_id != 0:

        raise ValueError(
            f"Found {missing_session_id} events with missing session_id."
        )

    # --------------------------------------------------------
    # Missing customer IDs
    # --------------------------------------------------------

    missing_customer_id = (
        sessionized_events
        .filter(F.col("customer_id").isNull())
        .count()
    )

    if missing_customer_id != 0:

        raise ValueError(
            f"Found {missing_customer_id} events with missing customer_id."
        )

    # --------------------------------------------------------
    # Missing timestamps
    # --------------------------------------------------------

    missing_timestamp = (
        sessionized_events
        .filter(F.col("timestamp").isNull())
        .count()
    )

    if missing_timestamp != 0:

        raise ValueError(
            f"Found {missing_timestamp} events with missing timestamp."
        )

    # --------------------------------------------------------
    # Missing event sequence
    # --------------------------------------------------------

    missing_sequence = (
        sessionized_events
        .filter(F.col("event_sequence").isNull())
        .count()
    )

    if missing_sequence != 0:

        raise ValueError(
            f"Found {missing_sequence} events without event_sequence."
        )

    # --------------------------------------------------------
    # Number of sessions
    # --------------------------------------------------------

    session_count = (
        sessionized_events
        .select("session_id")
        .distinct()
        .count()
    )

    summary_count = session_summary.count()

    if session_count != summary_count:

        raise ValueError(
            "Session summary count does not match "
            f"sessionized session count: "
            f"{session_count} != {summary_count}"
        )

    # --------------------------------------------------------
    # Verify chronological ordering
    # --------------------------------------------------------

    previous_timestamp_window = (
        Window
        .partitionBy("session_id")
        .orderBy("event_sequence")
    )

    checked = (
        sessionized_events
        .withColumn(
            "_previous_timestamp",
            F.lag("timestamp").over(
                previous_timestamp_window
            )
        )
        .filter(
            F.col("_previous_timestamp").isNotNull()
            &
            (
                F.col("timestamp")
                < F.col("_previous_timestamp")
            )
        )
    )

    ordering_problems = checked.count()

    if ordering_problems != 0:

        raise ValueError(
            f"Found {ordering_problems} chronological ordering problems."
        )

    return True


# ============================================================
# COMPLETE SESSIONIZATION STAGE
# ============================================================

def run_sessionization(
    events: DataFrame,
    sessions: DataFrame
):
    """
    Complete sessionization stage.

    Returns:
        sessionized_events
        session_summary
    """

    sessionized_events = create_sessionized_events(
        events,
        sessions
    )

    session_summary = create_session_summary(
        sessionized_events
    )

    validate_sessionization(
        events,
        sessionized_events,
        session_summary
    )

    return (
        sessionized_events,
        session_summary
    )