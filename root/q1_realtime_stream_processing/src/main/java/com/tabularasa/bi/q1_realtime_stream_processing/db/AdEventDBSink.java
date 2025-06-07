package com.tabularasa.bi.q1_realtime_stream_processing.db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.ForeachWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.Serializable;
import java.sql.PreparedStatement;

@Component
public class AdEventDBSink implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(AdEventDBSink.class);
    private static final long serialVersionUID = 1L;
    
    @Value("${spring.datasource.url}")
    private String dbUrl;
    
    @Value("${spring.datasource.username}")
    private String dbUsername;
    
    @Value("${spring.datasource.password}")
    private String dbPassword;

    public AdEventDBSink() {
        // Required empty constructor for Spring
    }

    /**
     * Creates a ForeachWriter for saving aggregated campaign data to PostgreSQL
     */
    public ForeachWriter<Row> createSinkWriter() {
        return new ForeachWriter<Row>() {
            private Connection connection;
            private static final String INSERT_SQL = 
                "INSERT INTO aggregated_campaign_stats " +
                "(campaign_id, event_type, window_start_time, event_count, total_bid_amount, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (campaign_id, event_type, window_start_time) " +
                "DO UPDATE SET " +
                "event_count = EXCLUDED.event_count, " +
                "total_bid_amount = EXCLUDED.total_bid_amount, " +
                "updated_at = EXCLUDED.updated_at";

            @Override
            public boolean open(long partitionId, long epochId) {
                try {
                    logger.debug("Opening database connection for partition: {}, epoch: {}", partitionId, epochId);
                    connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
                    return true;
                } catch (SQLException e) {
                    logger.error("Failed to open database connection", e);
                    return false;
                }
            }

            @Override
            public void process(Row row) {
                try (PreparedStatement stmt = connection.prepareStatement(INSERT_SQL)) {
                    String campaignId = row.getAs("campaign_id");
                    String eventType = row.getAs("event_type");
                    java.sql.Timestamp windowStart = row.getAs("window_start_time");
                    long eventCount = row.getAs("event_count");
                    double totalBidAmount = row.getAs("total_bid_amount");
                    java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());

                    stmt.setString(1, campaignId);
                    stmt.setString(2, eventType);
                    stmt.setTimestamp(3, windowStart);
                    stmt.setLong(4, eventCount);
                    stmt.setDouble(5, totalBidAmount);
                    stmt.setTimestamp(6, now);

                    int updated = stmt.executeUpdate();
                    logger.debug("Stored aggregated stats for campaign {}, type {}: {} rows affected", campaignId, eventType, updated);
                } catch (SQLException e) {
                    logger.error("Error storing aggregated stats", e);
                }
            }

            @Override
            public void close(Throwable errorOrNull) {
                if (connection != null) {
                    try {
                        connection.close();
                        logger.debug("Database connection closed");
                    } catch (SQLException e) {
                        logger.error("Error closing database connection", e);
                    }
                }
                if (errorOrNull != null) {
                    logger.error("Error in sink writer", errorOrNull);
                }
            }
        };
    }

    /**
     * Saves a Dataset with aggregated campaign data to the database
     */
    public void saveAggregatedCampaignStats(Dataset<Row> aggregatedStats) {
        if (aggregatedStats == null || aggregatedStats.isEmpty()) {
            logger.info("No aggregated stats to save");
            return;
        }
        try {
            logger.info("Saving {} aggregated campaign stats records to database", aggregatedStats.count());
            aggregatedStats.write()
                .format("jdbc")
                .option("url", dbUrl)
                .option("dbtable", "aggregated_campaign_stats")
                .option("user", dbUsername)
                .option("password", dbPassword)
                .mode("append")
                .save();
            logger.info("Successfully saved aggregated campaign stats to database");
        } catch (Exception e) {
            logger.error("Failed to save aggregated campaign stats to database", e);
        }
    }
} 