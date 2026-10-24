
import math
import os
import sys
import threading
import traceback
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session

from .database import Base, engine, get_db, SessionLocal
from .models import (
    PipelineRun,
    AnalyticsResult,
    FunnelSummary,
    FunnelTimeMetric,
    FunnelShortcut,
)

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

pipeline_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    Base.metadata.create_all(bind=engine)
    yield
    engine.dispose()


app = FastAPI(
    title="CLICKINSIGHT API",
    version="1.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "CLICKINSIGHT API is running"}


@app.get("/api/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "healthy", "database": "connected"}


@app.get("/api/runs")
def get_runs(db: Session = Depends(get_db)):
    runs = (
        db.query(PipelineRun)
        .order_by(PipelineRun.run_id.desc())
        .limit(50)
        .all()
    )
    return [
        {
            "run_id": r.run_id,
            "status": r.status,
            "started_at": r.started_at,
            "completed_at": r.completed_at,
            "error_message": r.error_message,
        }
        for r in runs
    ]


@app.get("/api/results")
def get_results(db: Session = Depends(get_db)):
    latest = (
        db.query(PipelineRun)
        .filter(PipelineRun.status == "SUCCESS")
        .order_by(PipelineRun.run_id.desc())
        .first()
    )

    result_types = [
        "funnel_summary",
        "funnel_time_metrics",
        "funnel_shortcuts",
        "journey_summary",
        "journey_transitions",
        "common_journeys",
        "journey_segments",
        "journey_length_distribution",
        "entry_events",
        "exit_events",
        "repeated_behavior",
    ]

    if latest is None:
        return {
            "run_id": None,
            **{key: [] for key in result_types},
        }

    run_id = latest.run_id
    stored = {
        r.result_type: r.result_data
        for r in db.query(AnalyticsResult).all()
    }

    funnel_summary = [
        {
            "stage": r.stage,
            "stage_order": r.stage_order,
            "sessions": r.sessions,
            "previous_stage_sessions": r.previous_stage_sessions,
            "conversion_rate": r.conversion_rate,
            "dropoff_rate": r.dropoff_rate,
        }
        for r in db.query(FunnelSummary)
        .filter_by(run_id=run_id)
        .order_by(FunnelSummary.stage_order)
        .all()
    ]

    funnel_time_metrics = [
        {
            "metric": r.metric,
            "count": r.count,
            "mean_minutes": r.mean_minutes,
            "median_minutes": r.median_minutes,
            "p25_minutes": r.p25_minutes,
            "p75_minutes": r.p75_minutes,
            "p90_minutes": r.p90_minutes,
            "stddev_minutes": r.stddev_minutes,
        }
        for r in db.query(FunnelTimeMetric)
        .filter_by(run_id=run_id)
        .order_by(FunnelTimeMetric.id)
        .all()
    ]

    funnel_shortcuts = [
        {"path": r.path, "sessions": r.sessions}
        for r in db.query(FunnelShortcut)
        .filter_by(run_id=run_id)
        .order_by(FunnelShortcut.id)
        .all()
    ]

    response = {"run_id": run_id}

    for key in result_types:
        response[key] = stored.get(key, [])

    response["funnel_summary"] = (
        funnel_summary or stored.get("funnel_summary", [])
    )
    response["funnel_time_metrics"] = (
        funnel_time_metrics or stored.get("funnel_time_metrics", [])
    )
    response["funnel_shortcuts"] = (
        funnel_shortcuts or stored.get("funnel_shortcuts", [])
    )

    return response


def dataframe_records(dataframe):
    rows = [row.asDict(recursive=True) for row in dataframe.collect()]

    def clean(value):
        if isinstance(value, dict):
            return {str(k): clean(v) for k, v in value.items()}

        if isinstance(value, (list, tuple)):
            return [clean(v) for v in value]

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if isinstance(value, Decimal):
            value = float(value)

        if isinstance(value, float):
            return value if math.isfinite(value) else None

        if value is None or isinstance(value, (str, int, bool)):
            return value

        if hasattr(value, "item"):
            try:
                return clean(value.item())
            except (TypeError, ValueError):
                pass

        return str(value)

    return clean(rows)


def safe_float(value):
    if value is None:
        return None

    number = float(value)
    return number if math.isfinite(number) else None


def persist_funnel_rows(db, run_id, payload):
    for item in payload["funnel_summary"]:
        db.add(
            FunnelSummary(
                run_id=run_id,
                stage=item["stage"],
                stage_order=int(item["stage_order"]),
                sessions=int(item["sessions"]),
                previous_stage_sessions=(
                    int(item["previous_stage_sessions"])
                    if item.get("previous_stage_sessions") is not None
                    else None
                ),
                conversion_rate=safe_float(
                    item.get("conversion_rate")
                ),
                dropoff_rate=safe_float(item.get("dropoff_rate")),
            )
        )

    for item in payload["funnel_time_metrics"]:
        db.add(
            FunnelTimeMetric(
                run_id=run_id,
                metric=item["metric"],
                count=int(item["count"]),
                mean_minutes=safe_float(item.get("mean_minutes")),
                median_minutes=safe_float(item.get("median_minutes")),
                p25_minutes=safe_float(item.get("p25_minutes")),
                p75_minutes=safe_float(item.get("p75_minutes")),
                p90_minutes=safe_float(item.get("p90_minutes")),
                stddev_minutes=safe_float(item.get("stddev_minutes")),
            )
        )

    for item in payload["funnel_shortcuts"]:
        db.add(
            FunnelShortcut(
                run_id=run_id,
                path=item["path"],
                sessions=int(item["sessions"]),
            )
        )


def persist_latest_analytics(db, run_id, payload):
    # This replacement happens within the caller's transaction.
    # A rollback restores the previously published dashboard results.
    db.query(FunnelSummary).delete(synchronize_session=False)
    db.query(FunnelTimeMetric).delete(synchronize_session=False)
    db.query(FunnelShortcut).delete(synchronize_session=False)
    db.query(AnalyticsResult).delete(synchronize_session=False)

    persist_funnel_rows(db, run_id, payload)

    now = datetime.utcnow()

    for result_type, result_data in payload.items():
        statement = insert(AnalyticsResult).values(
            result_type=result_type,
            result_data=result_data,
            updated_at=now,
        )
        statement = statement.on_duplicate_key_update(
            result_data=statement.inserted.result_data,
            updated_at=statement.inserted.updated_at,
        )
        db.execute(statement)


@app.post("/api/analyze")
def analyze():
    if not pipeline_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="Analysis already running.",
        )

    db = SessionLocal()
    spark = None
    run_id = None

    try:
        run = PipelineRun(status="RUNNING")
        db.add(run)
        db.commit()
        db.refresh(run)
        run_id = run.run_id

        from preprocessing.preprocess import (
            create_spark_session,
            load_raw_datasets,
            preprocess_all_datasets,
            validate_preprocessed_data,
        )
        from sessionization.sessionize import run_sessionization
        from funnel.funnel import run_funnel
        from journey.journey import run_journey_analysis
        from journey_quality_validation import (
            validate_dashboard_results,
        )

        spark = create_spark_session()
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        spark.conf.set("spark.default.parallelism", "4")
        spark.conf.set("spark.sql.adaptive.enabled", "true")

        raw = load_raw_datasets(spark)
        processed = preprocess_all_datasets(raw)
        validate_preprocessed_data(processed)

        sessionized_events, session_summary = run_sessionization(
            processed["events"],
            processed["sessions"],
        )

        funnel = run_funnel(sessionized_events)

        if not isinstance(funnel, dict):
            raise RuntimeError(
                "run_funnel() returned an unexpected result."
            )

        journey = run_journey_analysis(sessionized_events)

        validate_dashboard_results(
            processed["events"].count(),
            sessionized_events.count(),
            session_summary.count(),
            journey,
        )

        # Persist dashboard aggregates, not all 120,000 session-level journeys.
        payload = {
            "funnel_summary": dataframe_records(
                funnel["funnel_summary"]
            ),
            "funnel_time_metrics": dataframe_records(
                funnel["funnel_time_metrics"]
            ),
            "funnel_shortcuts": dataframe_records(
                funnel["funnel_shortcuts"]
            ),
            "journey_summary": dataframe_records(
                journey["summary"]
            ),
            "journey_transitions": dataframe_records(
                journey["transitions"]
            ),
            "common_journeys": dataframe_records(
                journey["common_journeys"]
            ),
            "journey_segments": dataframe_records(
                journey["segments"]
            ),
            "journey_length_distribution": dataframe_records(
                journey["journey_length_distribution"]
            ),
            "entry_events": dataframe_records(
                journey["entry_events"]
            ),
            "exit_events": dataframe_records(
                journey["exit_events"]
            ),
            "repeated_behavior": dataframe_records(
                journey["repeated_behavior"]
            ),
        }

        # Results and SUCCESS status are committed together.
        persist_latest_analytics(db, run_id, payload)

        run_to_publish = (
            db.query(PipelineRun).filter_by(run_id=run_id).one()
        )
        run_to_publish.status = "SUCCESS"
        run_to_publish.completed_at = datetime.utcnow()
        run_to_publish.error_message = None

        db.commit()

        return {
            "message": (
                "Funnel and journey analysis completed successfully."
            ),
            "run_id": run_id,
            "status": "SUCCESS",
            "quality_validation": "PASSED",
            "results": payload,
        }

    except HTTPException:
        db.rollback()
        raise

    except Exception as exc:
        db.rollback()

        if run_id is not None:
            failed = (
                db.query(PipelineRun)
                .filter_by(run_id=run_id)
                .first()
            )

            if failed is not None:
                failed.status = "FAILED"
                failed.completed_at = datetime.utcnow()
                failed.error_message = (
                    f"{type(exc).__name__}: {exc}\n"
                    + traceback.format_exc()[-6000:]
                )
                db.commit()

        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {type(exc).__name__}: {exc}",
        ) from exc

    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass

        db.close()
        pipeline_lock.release()
