from datetime import datetime

from sqlalchemy import (
    BigInteger, Column, DateTime, Float, ForeignKey,
    String, Text, UniqueConstraint
)
from sqlalchemy.dialects.mysql import JSON

from .database import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id = Column(BigInteger, primary_key=True, autoincrement=True)
    status = Column(String(20), nullable=False, default="RUNNING")
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)


class AnalyticsResult(Base):
    __tablename__ = "analytics_results"

    result_type = Column(String(100), primary_key=True)
    result_data = Column(JSON, nullable=False)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class FunnelSummary(Base):
    __tablename__ = "funnel_summary"
    __table_args__ = (
        UniqueConstraint("run_id", "stage_order", name="uq_funnel_run_stage"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(
        BigInteger, ForeignKey("pipeline_runs.run_id"), nullable=False, index=True
    )
    stage = Column(String(100), nullable=False)
    stage_order = Column(BigInteger, nullable=False)
    sessions = Column(BigInteger, nullable=False)
    previous_stage_sessions = Column(BigInteger, nullable=True)
    conversion_rate = Column(Float, nullable=True)
    dropoff_rate = Column(Float, nullable=True)


class FunnelTimeMetric(Base):
    __tablename__ = "funnel_time_metrics"
    __table_args__ = (
        UniqueConstraint("run_id", "metric", name="uq_funnel_time_run_metric"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(
        BigInteger, ForeignKey("pipeline_runs.run_id"), nullable=False, index=True
    )
    metric = Column(String(150), nullable=False)
    count = Column(BigInteger, nullable=False)
    mean_minutes = Column(Float, nullable=True)
    median_minutes = Column(Float, nullable=True)
    p25_minutes = Column(Float, nullable=True)
    p75_minutes = Column(Float, nullable=True)
    p90_minutes = Column(Float, nullable=True)
    stddev_minutes = Column(Float, nullable=True)


class FunnelShortcut(Base):
    __tablename__ = "funnel_shortcuts"
    __table_args__ = (
        UniqueConstraint("run_id", "path", name="uq_funnel_shortcut_run_path"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(
        BigInteger, ForeignKey("pipeline_runs.run_id"), nullable=False, index=True
    )
    path = Column(String(500), nullable=False)
    sessions = Column(BigInteger, nullable=False)
