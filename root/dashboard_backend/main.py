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
    revenue_euro: float = Field(..., ge=0)
    roas: float = Field(..., ge=0)
    cpa: float = Field(..., ge=0)
    conversion_rate: float = Field(..., ge=0)

class ROIDailyPoint(BaseModel):
    window_start_time: str
    roi: float

class PipelineHealth(BaseModel):
    data_freshness_seconds: int = Field(..., ge=0)
    etl_success_rate: float = Field(..., ge=0, le=100)
    data_quality_score: float = Field(..., ge=0, le=100)

class CampaignPerformance(BaseModel):
    campaign_id: str
    impressions: int
    clicks: int
    conversions: int
    spend_usd: float
    ctr: float

class PerformanceByDevice(BaseModel):
    device_type: str
    total_revenue: float
    conversion_rate: float

class RevenueByCategory(BaseModel):
    product_category_1: str
    total_revenue: float

class RevenueByBrand(BaseModel):
    product_brand: str
    total_revenue: float

class RevenueByAgeGroup(BaseModel):
    product_age_group: str
    total_revenue: float

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

def load_sql_query(filename: str, query_name: str) -> text:
    """Load a specific SQL query by its -- NAME: from a file."""
    sql_dir = BASE_DIR / "sql" / "dashboard"
    sql_file = sql_dir / filename

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found in {sql_dir} or its subdirectories: {filename}")

    sql_content = sql_file.read_text()
    
    queries = {}
    current_query_name = None
    current_lines = []

    for line in sql_content.split('\n'):
        # Check for our specific delimiter
        if line.strip().startswith('-- NAME:'):
            # If we were already building a query, save it
            if current_query_name and current_lines:
                queries[current_query_name] = '\n'.join(current_lines).strip()
            
            # Start a new query
            current_query_name = line.split(':', 1)[1].strip()
            current_lines = []
        elif current_query_name:
            # Add line to the current query being built
            current_lines.append(line)
            
    # Save the last query in the file
    if current_query_name and current_lines:
        queries[current_query_name] = '\n'.join(current_lines).strip()

    if query_name in queries:
        return text(queries[query_name])
    else:
        raise ValueError(f"Query '{query_name}' not found in {filename}")

# --- SQL Queries ----------------------------------------------------------------------------------

try:
    # Try to load queries from SQL files
    KPI_QUERY = load_sql_query("kpi_queries.sql", "KPI Query")
    ROI_TREND_QUERY = load_sql_query("kpi_queries.sql", "ROI Trend Query")
    FRESHNESS_QUERY = load_sql_query("kpi_queries.sql", "Freshness Query")
    CAMPAIGN_PERFORMANCE_QUERY = load_sql_query("kpi_queries.sql", "Campaign Performance Query")
    DEVICE_PERFORMANCE_QUERY = load_sql_query("kpi_queries.sql", "Performance by Device Type")
    CATEGORY_REVENUE_QUERY = load_sql_query("kpi_queries.sql", "Revenue by Product Category")
    BRAND_REVENUE_QUERY = load_sql_query("kpi_queries.sql", "Performance by Product Brand")
    AGE_GROUP_REVENUE_QUERY = load_sql_query("kpi_queries.sql", "Performance by Product Age Group")
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
    CAMPAIGN_PERFORMANCE_QUERY = text("SELECT * FROM aggregated_campaign_stats ORDER BY spend_usd DESC LIMIT 10;")
    # Note: No fallback for new queries, they must exist in the SQL file
    BRAND_REVENUE_QUERY = None
    AGE_GROUP_REVENUE_QUERY = None

# --- Endpoints ------------------------------------------------------------------------------------

@app.get("/api/kpis", response_model=KPIResponse)
async def read_kpis(session: AsyncSession = Depends(get_session)) -> KPIResponse:
    """Aggregate high-level KPIs for dashboards."""
    try:
        result = await session.execute(KPI_QUERY)
        row = result.one_or_none()
        if not row:
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT, detail="No KPI data yet")

        impressions, clicks, conversions, spend_usd, revenue_euro, ctr, roas = row
        
        cpa = 0 if conversions == 0 else spend_usd / conversions
        conversion_rate = 0 if clicks == 0 else conversions / clicks

        return KPIResponse(
            impressions=int(impressions),
            clicks=int(clicks),
            conversions=int(conversions),
            ctr=float(ctr),
            spend_usd=float(spend_usd),
            revenue_euro=float(revenue_euro),
            roas=float(roas),
            cpa=float(cpa),
            conversion_rate=float(conversion_rate)
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

@app.get("/api/campaign_performance", response_model=List[CampaignPerformance])
async def read_campaign_performance(session: AsyncSession = Depends(get_session)) -> List[CampaignPerformance]:
    """Return performance metrics for top 10 campaigns by spend."""
    try:
        result = await session.execute(CAMPAIGN_PERFORMANCE_QUERY)
        rows = result.all()
    except (ProgrammingError, OperationalError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Campaign data unavailable — pipeline not started or table missing.")
    
    return [CampaignPerformance(
        campaign_id=row.campaign_id,
        impressions=row.impressions or 0,
        clicks=row.clicks or 0,
        conversions=row.conversions or 0,
        spend_usd=row.spend_usd or 0,
        ctr=row.ctr or 0
    ) for row in rows]

@app.get("/api/performance/by_device", response_model=List[PerformanceByDevice])
async def read_performance_by_device(session: AsyncSession = Depends(get_session)) -> List[PerformanceByDevice]:
    """Return performance metrics aggregated by device type."""
    try:
        result = await session.execute(DEVICE_PERFORMANCE_QUERY)
        rows = result.all()
    except (ProgrammingError, OperationalError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Device performance data unavailable.")
    return [PerformanceByDevice(
        device_type=row.device_type,
        total_revenue=row.total_revenue or 0,
        conversion_rate=row.conversion_rate or 0
    ) for row in rows]

@app.get("/api/performance/by_category", response_model=List[RevenueByCategory])
async def read_revenue_by_category(session: AsyncSession = Depends(get_session)) -> List[RevenueByCategory]:
    """Return revenue aggregated by product category."""
    try:
        result = await session.execute(CATEGORY_REVENUE_QUERY)
        rows = result.all()
    except (ProgrammingError, OperationalError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Category revenue data unavailable.")
    return [RevenueByCategory(
        product_category_1=row.product_category_1,
        total_revenue=row.total_revenue or 0
    ) for row in rows]

@app.get("/api/performance/by_brand", response_model=List[RevenueByBrand])
async def read_revenue_by_brand(session: AsyncSession = Depends(get_session)) -> List[RevenueByBrand]:
    """Return revenue aggregated by product brand."""
    if BRAND_REVENUE_QUERY is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not loaded from file.")
    try:
        result = await session.execute(BRAND_REVENUE_QUERY)
        rows = result.all()
    except (ProgrammingError, OperationalError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Brand revenue data unavailable.")
    return [RevenueByBrand(
        product_brand=row.product_brand,
        total_revenue=row.total_revenue or 0
    ) for row in rows]

@app.get("/api/performance/by_age_group", response_model=List[RevenueByAgeGroup])
async def read_revenue_by_age_group(session: AsyncSession = Depends(get_session)) -> List[RevenueByAgeGroup]:
    """Return revenue aggregated by product age group."""
    if AGE_GROUP_REVENUE_QUERY is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not loaded from file.")
    try:
        result = await session.execute(AGE_GROUP_REVENUE_QUERY)
        rows = result.all()
    except (ProgrammingError, OperationalError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Age group revenue data unavailable.")
    return [RevenueByAgeGroup(
        product_age_group=row.product_age_group,
        total_revenue=row.total_revenue or 0
    ) for row in rows]

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