# Hypothetical Data Engineering Challenge

**Subject**: Data Engineering – Technical Assessment

Greetings,

Thank you for taking the time to tackle this challenge. Its purpose is to evaluate core data-engineering and backend skills and to serve as common ground for the technical conversation that follows.

**General Notes:**

- Submissions are required to be primarily in **Java**. For specific questions (e.g., SQL), use the requested language.
- **Apache Spark** should be used for data processing tasks where specified.
- Candidates are encouraged to use any resource at their disposal (online, documentation, etc.). 
- Code can be delivered by a link to a Git repository (e.g., GitHub, GitLab). Ensure the repository is accessible.
- There is no strict time limit – complete the test and send it back at the earliest convenience. It is typically expected to be a weekend project.
- Use best judgment! A lot of freedom is left to candidates to answer the questions: if in doubt, follow the "do something reasonable" approach.
- Where needed, email the hiring manager for clarifications.
- Include a brief README with instructions on how to build and run the solution(s), and any assumptions made.

---

**Question 1: Real-time Event Stream Processing with Spark**

**Background:** Processing massive streams of real-time events (e.g., ad impressions, clicks, content views) efficiently is crucial in data-intensive environments.

**Task:**
Develop a Java application using Apache Spark (Structured Streaming) that simulates processing a stream of ad campaign events. Each event has the following schema:
`{ "timestamp": "YYYY-MM-DDTHH:mm:ssZ", "campaign_id": "string", "event_type": "string" (e.g., "impression", "click", "conversion"), "user_id": "string", "bid_amount_usd": double }`

The application should:
1. **Generate or Read a Stream:** Either generate a mock stream of events within the Spark application (e.g., using `RateStreamSource` or by generating random data) or read from a sample JSONL/CSV file in a streaming fashion. Aim for a reasonable volume to demonstrate stream processing.
2. **Process Events:**
   - Aggregate event counts and total `bid_amount_usd` per `campaign_id` and `event_type` within 1-minute tumbling windows.
   - Handle potential out-of-order events using watermarking (e.g., allow for 10 seconds of lateness).
3. **Store Aggregations:** Write the aggregated results (window_start_time, campaign_id, event_type, event_count, total_bid_amount) to an in-memory HSQLDB table (or a similar embedded database). Define the schema for this table. (Optionally note in the README how the solution could connect to a more production-like database like PostgreSQL or a columnar database.)
4. **API Endpoint (Bonus):** Implement a simple REST API endpoint using Spring Boot (or a lightweight framework like Javalin/SparkJava) that exposes the aggregated data. For example:
   - `GET /campaign-stats/{campaign_id}?startTime=<timestamp>&endTime=<timestamp>`: Returns aggregated stats for a given campaign within a time range.

**Deliverables:**
- Java/Spark source code.
- SQL schema for the database table.
- Brief explanation of design choices, especially regarding stream generation, windowing, watermarking, and database interaction.
- If attempting the bonus, provide the API code and an example query.

---

**Question 2: Advanced SQL for Ad Performance Analysis**

**Background:** BI teams frequently need to analyze ad performance across various dimensions.

**Task:**
Assume a conceptual data warehouse table named `ad_performance_hourly` with the following schema, representing hourly aggregated data (data loading for this table does not need to be implemented, just use it for the query):

`ad_performance_hourly`
- `hour_timestamp` (TIMESTAMP): The specific hour of aggregation.
- `campaign_id` (VARCHAR)
- `publisher_id` (VARCHAR)
- `country_code` (VARCHAR(2))
- `impressions` (BIGINT)
- `clicks` (BIGINT)
- `conversions` (BIGINT)
- `spend_usd` (DECIMAL(10,2))

Write a **single SQL query** to identify the top 3 performing `campaign_id`s for each `country_code` on a specific day (`YYYY-MM-DD`, e.g., '2024-07-15'). "Top performing" is defined by the highest Click-Through Rate (CTR = clicks / impressions). If impressions are zero, CTR should be considered zero.
Additionally, for these top campaigns, include their total `spend_usd` and total `conversions` for that day and country.

**Rules & Considerations:**
- Handle cases where a campaign might have no impressions (to avoid division by zero).
- Ensure the query correctly ranks campaigns within each country.
- The output should include: `country_code`, `campaign_id`, `rank_in_country`, `ctr`, `total_spend_usd`, `total_conversions`.

**Deliverables:**
- The single SQL query.
- A brief explanation of the approach, particularly how ranking and aggregation were handled.

---

**Question 3: Java Code Refactoring & Optimization**

**Background:** Maintaining high-quality, efficient code is essential for large-scale systems.

**Task:**
The provided Java class (`DataProcessor.java`) contains performance and design issues. Refactor it to improve correctness, efficiency, readability and adherence to modern Java best practices.

**Sample Code (Provided for Context):**
```java
// Simulates processing records and finding duplicates
public class DataProcessor {
    private List<String> processedRecords = new ArrayList<>();
    private Map<String, Integer> recordCounts = new HashMap<>();

    public List<String> processBatch(List<Map<String, String>> batch) {
        List<String> duplicatesInBatch = new ArrayList<>();
        for (Map<String, String> record : batch) {
            String key = record.get("id") + ':' + record.get("type");
            if (!processedRecords.add(key)) {
                duplicatesInBatch.add(key);
            }
            recordCounts.merge(key, 1, Integer::sum);
        }
        return duplicatesInBatch;
    }
}
```

**Deliverables:**
- Refactored `DataProcessor.java`.
- Short rationale of changes (performance, memory, design).

---

**Question 4: Data Ingestion API Design & Client**

**Background:** External systems push event data into the platform; the interface must be robust and idempotent.

**Task:**
1. **Design** a REST endpoint (e.g., `POST /v1/bi-events`) accepting a batch of events (`timestamp`, `event_name`, flexible `attributes` JSON). Define success (`202 Accepted`) and error responses, describe idempotency, basic auth, and validation considerations.
2. **Implement** a Java client that assembles 2-3 mock events and submits them to a mocked endpoint (e.g., `http://localhost:12345/mock-api/v1/bi-events`) showing handling of success and error responses.

**Deliverables:**
- API design summary (can live in README).
- Java client source code.
- Explanation of payload construction and error handling.

---

We look forward to reviewing the submission!

Best regards,
The Hiring Team