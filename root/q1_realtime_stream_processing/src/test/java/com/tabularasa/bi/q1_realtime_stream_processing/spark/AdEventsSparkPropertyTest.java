package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.time.*;
import java.util.*;
import java.util.stream.*;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;

class AdEventsSparkPropertyTest {
    private SparkSession spark;
    private StructType schema;

    @BeforeEach
    void setUp() {
        spark = SparkSession.builder().master("local[2]").appName("AdEventsPropertyTest").getOrCreate();
        schema = new StructType()
                .add("timestamp", "string")
                .add("campaign_id", "string")
                .add("event_type", "string")
                .add("user_id", "string")
                .add("bid_amount_usd", "double");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) spark.stop();
    }

    @Test
    void propertyBasedAggregationTest() {
        qt().forAll(
                lists().of(eventGen()).ofSizeBetween(100, 500)
        ).check(events -> {
            List<Row> rows = events.stream().map(this::toRow).collect(Collectors.toList());
            Dataset<Row> df = spark.createDataFrame(rows, schema)
                    .withColumn("event_timestamp", functions.to_timestamp(functions.col("timestamp")));
            Dataset<Row> agg = df.groupBy(
                    functions.window(functions.col("event_timestamp"), "1 minute"),
                    functions.col("campaign_id"),
                    functions.col("event_type")
            ).agg(
                    functions.count("*").alias("event_count"),
                    functions.sum("bid_amount_usd").alias("total_bid_amount")
            );
            List<Row> result = agg.collectAsList();
            // Invariant: sum of event_count == input size
            long totalCount = result.stream().mapToLong(r -> r.getAs("event_count")).sum();
            if (totalCount != events.size()) return false;
            // Invariant: all total_bid_amount >= 0
            if (result.stream().anyMatch(r -> r.getAs("total_bid_amount") == null || ((Double) r.getAs("total_bid_amount")) < 0)) return false;
            // Invariant: no duplicate (window, campaign_id, event_type)
            Set<String> keys = new HashSet<>();
            for (Row r : result) {
                String key = r.getAs("window").toString() + r.getAs("campaign_id") + r.getAs("event_type");
                if (!keys.add(key)) return false;
            }
            return true;
        });
    }

    private org.quicktheories.core.Gen<Map<String, Object>> eventGen() {
        org.quicktheories.core.Gen<String> timestampGen = longs().between(0L, 60L * 60L).map(
                sec -> LocalDateTime.now().minusSeconds(sec).toString());
        org.quicktheories.core.Gen<String> campaignIdGen = strings().basicLatinAlphabet().ofLengthBetween(8, 8);
        org.quicktheories.core.Gen<String> eventTypeGen = integers().between(0, 2).map(i -> List.of("impression", "click", "conversion").get(i));
        org.quicktheories.core.Gen<String> userIdGen = strings().basicLatinAlphabet().ofLengthBetween(8, 8);
        org.quicktheories.core.Gen<Double> bidAmountGen = doubles().between(0.0, 100.0);
        return timestampGen.zip(campaignIdGen, eventTypeGen, userIdGen, bidAmountGen,
                (ts, cid, et, uid, bid) -> {
                    Map<String, Object> e = new HashMap<>();
                    e.put("timestamp", ts);
                    e.put("campaign_id", cid);
                    e.put("event_type", et);
                    e.put("user_id", uid);
                    e.put("bid_amount_usd", bid);
                    return e;
                });
    }

    private Row toRow(Map<String, Object> e) {
        return RowFactory.create(
                e.get("timestamp"),
                e.get("campaign_id"),
                e.get("event_type"),
                e.get("user_id"),
                Double.parseDouble(e.get("bid_amount_usd").toString())
        );
    }
}