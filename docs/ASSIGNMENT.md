# Hypothetical Senior BI Data Engineer Assignment

**Subject**: Senior BI Data Engineer - Home Assignment

Greetings,

Thank you for your interest in the Senior BI Data Engineer position. The next step in the process is a home assignment. The purpose of this test is to evaluate technical abilities in several aspects of data engineering and backend development. It will also provide common ground for discussion in the technical interview that will follow completion of the test.

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

**Background:** Maintaining high-quality, efficient code is essential for systems.

**Task:**
A provided Java class (`DataProcessor.java`) has several coding inefficiencies, potential memory issues, and design flaws. Review the code, identify these issues, and refactor it to improve its correctness, efficiency, readability, and adherence to Java best practices.

**Sample Code (Provided for Context):**
```java
// Simulates processing records and finding duplicates
public class DataProcessor {
    private List<String> processedRecords = new ArrayList<String>();
    private Map<String, Integer> recordCounts = new HashMap<String, Integer>();

    // Processes a batch of records. A record is considered a duplicate
    // if its "key" has been seen before.
    // Returns a list of keys that were duplicates in this batch.
    public List<String> processBatch(List<Map<String, String>> batch) {
        List<String> duplicatesInBatch = new ArrayList<String>();
        for (int i = 0; i < batch.size(); i++) { // Inefficient loop
            Map<String, String> record = batch.get(i);
            String key = record.get("id") + ":" + record.get("type"); // Inefficient string concatenation

            // Check for duplicates
            for (String processed : processedRecords) { // Very inefficient check
                if (processed.equals(key)) {
                    duplicatesInBatch.add(key);
                    break;
                }
            }

            if (!duplicatesInBatch.contains(key)) { // Redundant check if already added
                 processedRecords.add(key); // Potential for very large list
            }

            // Update counts
            if (recordCounts.get(key) == null) {
                recordCounts.put(key, new Integer(1)); // Autoboxing, new Integer() is deprecated
            } else {
                Integer count = recordCounts.get(key);
                recordCounts.put(key, new Integer(count.intValue() + 1));
            }
        }
        return duplicatesInBatch;
    }

    // Additional methods omitted for brevity
}
```

**Deliverables:**
- The refactored `DataProcessor.java` class.
- A brief explanation of the issues identified and the changes made to address them. Focus on performance, memory usage, correctness, and design principles.

---

**Question 4: Data Ingestion API Design & Client**

**Background:** BI systems often need to ingest data from various external and internal APIs.

**Task:**
1. **Design a Simple Data Ingestion API:**
   Conceptually design a REST API endpoint that would allow external systems to push event data to a BI platform. Consider the following:
   - **Endpoint:** e.g., `POST /v1/bi-events`
   - **Request Payload:** Define a JSON structure for a batch of events. Each event should contain at least a `timestamp`, an `event_name`, and a flexible `attributes` field (e.g., a JSON object for event-specific details).
   - **Response:** What would a successful (e.g., `202 Accepted`) and error response (e.g., `400 Bad Request`, `500 Internal Server Error`) look like?
   - **Key Considerations:** Think about idempotency (how to handle retries of the same batch), authentication (briefly mention, no need to implement), and basic validation.

2. **Implement a Java API Client:**
   Write a simple Java client that can send a batch of mock events (matching the JSON structure designed) to a *mocked* version of this API endpoint.
   - **Note:** Server-side implementation of the API is not required.
   - The client should construct a batch of 2-3 sample events and attempt to "send" them. Simulate the HTTP call (e.g., by printing the request that *would* be sent, or by using a library like OkHttp/Apache HttpClient to send to a non-existent or mock server URL like `http://localhost:12345/mock-api/v1/bi-events`).
   - The client should demonstrate how it would handle a successful response and a potential error response from the (mocked) API.

**Deliverables:**
- A brief document or section in the README describing the API design (endpoint, request/response structure, considerations for idempotency, auth, validation).
- The Java source code for the API client.
- An explanation of how the client constructs the payload and handles potential API responses.

---

We look forward to reviewing the submission!

Best regards,
The Hiring Team
