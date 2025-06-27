package com.tabularasa.bi.q1_realtime_stream_processing.db;

import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.ForeachWriter;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * A custom Spark ForeachWriter to sink AdEvent data into a PostgreSQL database.
 */
@Slf4j
@Component
public class AdEventDBSink extends ForeachWriter<AdEvent> {

    private final String driver = "org.postgresql.Driver";
    private String url;
    private String user;
    private String password;

    private Connection connection;
    private PreparedStatement statement;

    /**
     * Constructor for manual instantiation.
     */
    public AdEventDBSink(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }
    
    // No-arg constructor for Spark serialization, though manual instantiation is preferred.
    public AdEventDBSink() {
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            // Using an idempotent INSERT...ON CONFLICT DO NOTHING to handle potential duplicates
            String query = "INSERT INTO ad_events (event_id, ad_creative_id, user_id, event_type, timestamp, bid_amount_usd) " +
                           "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING";
            statement = connection.prepareStatement(query);
            log.info("Opened database connection for partition {} and epoch {}", partitionId, epochId);
            return true;
        } catch (ClassNotFoundException | SQLException e) {
            log.error("Failed to open database connection for partition {} and epoch {}", partitionId, epochId, e);
            return false;
        }
    }

    @Override
    public void process(AdEvent value) {
        try {
            statement.setString(1, value.getEventId());
            statement.setString(2, value.getAdCreativeId());
            statement.setString(3, value.getUserId());
            statement.setString(4, value.getEventType());
            statement.setTimestamp(5, new Timestamp(value.getTimestamp().getTime()));
            statement.setBigDecimal(6, value.getBidAmountUsd());
            statement.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to process ad event with ID: {}", value.getEventId(), e);
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
            log.info("Closed database connection");
        } catch (SQLException e) {
            log.error("Failed to close database connection", e);
        }
    }
}