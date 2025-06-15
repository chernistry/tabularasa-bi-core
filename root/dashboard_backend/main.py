from __future__ import annotations

import os
from typing import List, Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, FileResponse
from pathlib import Path

# --- Database -------------------------------------------------------------------------------------

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://airflow:airflow@localhost:5432/airflow",
)

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def get_session() -> AsyncSession:  # pragma: no cover
    async with SessionLocal() as session:
        yield session

# --- Pydantic Schemas -----------------------------------------------------------------------------

class KPIResponse(BaseModel):
    impressions: int = Field(..., ge=0)
    clicks: int = Field(..., ge=0)
    conversions: int = Field(..., ge=0)
    ctr: float = Field(..., ge=0)
    spend_usd: float = Field(..., ge=0)

class ROIDailyPoint(BaseModel):
    window_start_time: str
    roi: float

class PipelineHealth(BaseModel):
    data_freshness_seconds: int = Field(..., ge=0)
    etl_success_rate: float = Field(..., ge=0, le=100)
    data_quality_score: float = Field(..., ge=0, le=100)

# --- FastAPI App ----------------------------------------------------------------------------------

app = FastAPI(title="TabulaRasa Dashboard API", version="0.1.0")

# --- Static & Home Page ---------------------------------------------------------------------------

# Project root is 2 levels up from this file (root/dashboard_backend/main.py)
BASE_DIR = Path(__file__).resolve().parents[2]

# Serve HTML dashboards located in <project>/dashboards
DASHBOARD_DIR = BASE_DIR / "dashboards"
if DASHBOARD_DIR.exists():
    app.mount("/dashboards", StaticFiles(directory=str(DASHBOARD_DIR), html=True), name="dashboards")

@app.get("/index.html", include_in_schema=False)
async def serve_root_index() -> FileResponse:  # pragma: no cover
    """Serve the landing page."""
    return FileResponse(str(BASE_DIR / "index.html"))

@app.get("/", include_in_schema=False)
async def index() -> RedirectResponse:  # pragma: no cover
    """Redirect root to landing page."""
    return RedirectResponse(url="/index.html")

# --- SQL Loading Helper ---------------------------------------------------------------------------

def load_sql_query(filename: str, query_name: str = None) -> text:
    """Load SQL query from file and return as SQLAlchemy text object."""
    sql_dir = BASE_DIR / "sql" / "dashboard"
    sql_file = sql_dir / filename
    
    if not sql_file.exists():
        # Попробуем fallback на старый путь
        sql_file = BASE_DIR / "sql" / filename
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    sql_content = sql_file.read_text()
    
    # If query_name is provided, extract just that query
    if query_name:
        # Simple parser for -- Query Name: format
        queries = {}
        current_query = None
        current_lines = []
        
        for line in sql_content.split('\n'):
            if line.strip().startswith('-- ') and ':' in line:
                if current_query and current_lines:
                    queries[current_query] = '\n'.join(current_lines).strip()
                    current_lines = []
                current_query = line.strip()[3:].split(':', 1)[0].strip()
            elif current_query:
                current_lines.append(line)
        
        if current_query and current_lines:
            queries[current_query] = '\n'.join(current_lines).strip()
            
        if query_name in queries:
            return text(queries[query_name])
        else:
            # Fallback to using the entire file
            return text(sql_content)
    
    return text(sql_content)

# --- SQL Queries ----------------------------------------------------------------------------------

try:
    # Try to load queries from SQL files
    KPI_QUERY = load_sql_query("kpi_queries.sql", "KPI Query")
    ROI_TREND_QUERY = load_sql_query("kpi_queries.sql", "ROI Trend Query")
    FRESHNESS_QUERY = load_sql_query("kpi_queries.sql", "Freshness Query")
except FileNotFoundError:
    # Fallback to hardcoded queries
    KPI_QUERY = text(
        """
        SELECT  COALESCE(SUM(event_count) FILTER (WHERE event_type = 'impression'), 0) AS impressions,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 'click'), 0)       AS clicks,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 'conversion'), 0)  AS conversions,
                COALESCE(SUM(total_bid_amount), 0)                                      AS spend_usd
        FROM    aggregated_campaign_stats;
        """
    )
    
    ROI_TREND_QUERY = text(
        """
        SELECT  window_start_time::date AS day,
                COALESCE(SUM(total_bid_amount) FILTER (WHERE event_type = 'conversion'), 0) /
                NULLIF(COALESCE(SUM(total_bid_amount) FILTER (WHERE event_type = 'impression'), 0), 0) AS roi
        FROM    aggregated_campaign_stats
        GROUP BY day
        ORDER BY day;
        """
    )
    
    FRESHNESS_QUERY = text("SELECT MAX(window_start_time) as latest_ts FROM aggregated_campaign_stats;")

# --- Endpoints ------------------------------------------------------------------------------------

@app.get("/api/kpis", response_model=KPIResponse)
async def read_kpis(session: AsyncSession = Depends(get_session)) -> KPIResponse:
    """Aggregate high-level KPIs for dashboards."""
    try:
        result = await session.execute(KPI_QUERY)
        row = result.one_or_none()
        if not row:
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT, detail="No KPI data yet")
        impressions, clicks, conversions, spend_usd = row
        ctr = 0 if impressions == 0 else round(clicks / impressions * 100, 2)
        return KPIResponse(
            impressions=int(impressions),
            clicks=int(clicks),
            conversions=int(conversions),
            ctr=ctr,
            spend_usd=float(spend_usd),
        )
    except (ProgrammingError, OperationalError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Database not ready — run pipeline or create tables.")

@app.get("/api/roi_trend", response_model=List[ROIDailyPoint])
async def read_roi_trend(session: AsyncSession = Depends(get_session)) -> List[ROIDailyPoint]:
    """Return daily ROI trend for line charts."""
    try:
        result = await session.execute(ROI_TREND_QUERY)
        rows = result.all()
    except (ProgrammingError, OperationalError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="ROI data unavailable — pipeline not started or table missing.")
    return [ROIDailyPoint(window_start_time=str(day), roi=round(roi or 0, 4)) for day, roi in rows]

@app.get("/api/pipeline_health", response_model=PipelineHealth)
async def read_pipeline_health(session: AsyncSession = Depends(get_session)) -> PipelineHealth:
    """Return minimal pipeline health metrics (placeholder)."""
    freshness_seconds = 9999
    try:
        result = await session.execute(FRESHNESS_QUERY)
        latest_ts = result.scalar_one_or_none()
        if latest_ts:
            # Make sure we handle timezone-aware and naive datetime objects correctly
            if latest_ts.tzinfo is None:
                # Convert naive datetime to timezone-aware
                latest_ts = latest_ts.replace(tzinfo=timezone.utc)
            freshness_seconds = int((datetime.now(timezone.utc) - latest_ts).total_seconds())
    except (ProgrammingError, OperationalError):
        pass  # Table missing => keep default freshness

    etl_success_rate = 99.9  # Placeholder until Airflow metrics available
    data_quality_score = 98.5  # Placeholder

    return PipelineHealth(
        data_freshness_seconds=freshness_seconds,
        etl_success_rate=etl_success_rate,
        data_quality_score=data_quality_score,
    )

# --- CLI entry ------------------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)