# Solution Details: Q1–Q4

---

## Q1: Spark Structured Streaming Implementation

### Overview

Apache Spark Structured Streaming is used to process real-time ad events from Kafka, aggregate them in 1-minute windows, and store results in PostgreSQL.

**Key points:**
- Read events from Kafka (`ad-events`)
- Watermarking (10 seconds) for late event handling
- Tumbling window (1 minute) on event timestamp
- Aggregation by `campaign_id`, `event_type`
- foreachBatch + upsert to PostgreSQL (`aggregated_campaign_stats`)
- Output mode: `update` (incremental writes)

**Sample code:**
```java
Dataset<Row> windowedDF = eventsDF
        .withWatermark("event_timestamp", "10 seconds")
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("campaign_id"),
            col("event_type")
        )
        .agg(
            count("*").alias("event_count"),
            sum("bid_amount_usd").alias("total_bid_amount")
        );
```

**Validation:**
- Spark UI: http://localhost:4040
- PostgreSQL: see commands in `scripts/run_q1_spark_mode.sh`
- REST API: `/api/v1/campaign-stats/{campaign_id}?startTime=...&endTime=...`

**See code comments and README for implementation details, tests, and improvement suggestions.**

---

## Q2: SQL Query Explanation

The SQL query finds the top-3 campaigns by CTR for each country per day, including total spend and conversions.

**Structure:**
- CTE `CampaignDailyPerformance`: daily aggregation
- CTE `CampaignCTR`: CTR calculation with zero-division protection
- CTE `RankedCampaigns`: ranking by CTR (and spend as tie-breaker)
- Final SELECT: only top-3 per country

**Notes:**
- Uses ROW_NUMBER() window function for ranking
- Handles edge-case: if impressions=0, CTR=0
- Result: aggregated metrics and ranks per country

---

## Q3: Java Refactoring Explanation

**Main improvements in DataProcessor:**
- Use HashSet instead of ArrayList for duplicate detection (O(1) vs O(N))
- String.join instead of string concatenation for keys
- Map.compute for incrementing instead of manual logic
- Collections.unmodifiableMap to protect internal state
- Improved null/malformed record handling
- Detailed Javadoc and comments

**Result:**
- Much better performance and readability
- Correct duplicate and unique record handling
- Easier to test and maintain

---

## Q4: API Design (Data Ingestion)

**POST /v1/bi-events**
- Accepts a batch of events with a unique batch_id
- JWT authentication
- Idempotency: repeated batch_id → 409 Conflict
- Validation of structure, types, business rules
- Limits: 1000 events/batch, 5MB, rate limit
- Asynchronous processing (202 Accepted)
- All responses include request_id for tracing

**See the original API doc (Q4) for request/response examples and edge-cases.**

---

**For all architecture and requirements details, see [docs/ASSIGNMENT.md](ASSIGNMENT.md) and [docs/mermaid_graph.md](mermaid_graph.md).** 