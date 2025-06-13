package com.tabularasa.bi.q1_realtime_stream_processing.db;

import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.spark.sql.ForeachWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * A Spark ForeachWriter to sink AdEvent data into a database.
 *
 * <p>This class is responsible for managing the database connection lifecycle
 * and writing individual AdEvent records to the 'ad_events' table.
 * It is designed to be serializable and used within a Spark Streaming query.
 */
@Component
public final class AdEventDBSink extends ForeachWriter<AdEvent> {

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AdEventDBSink.class);

    /**
     * The JDBC URL for the database connection.
     * Injected from application properties.
     */
    @Value("${spring.datasource.url}")
    private String url;

    /**
     * The username for the database connection.
     * Injected from application properties.
     */
    @Value("${spring.datasource.username}")
    private String username;

    /**
     * The password for the database connection.
     * Injected from application properties.
     */
    @Value("${spring.datasource.password}")
    private String password;

    /**
     * The database connection instance. It is transient to avoid serialization issues
     * as connections are not serializable and should be managed per partition.
     */
    private transient Connection connection;

    /**
     * The prepared statement for inserting ad events. It is transient for the same
     * reason as the connection.
     */
    private transient PreparedStatement statement;

    /**
     * The SQL query for inserting an ad event into the database.
     */
    private static final String INSERT_QUERY = "INSERT INTO ad_events "
            + "(event_id, ad_creative_id, user_id, event_type, event_timestamp, processing_timestamp) "
            + "VALUES (?, ?, ?, ?, ?, ?)";

    // Column indices for the prepared statement
    private static final int EVENT_ID_COL = 1;
    private static final int AD_CREATIVE_ID_COL = 2;
    private static final int USER_ID_COL = 3;
    private static final int EVENT_TYPE_COL = 4;
    private static final int EVENT_TIMESTAMP_COL = 5;
    private static final int PROCESSING_TIMESTAMP_COL = 6;


    /**
     * Called when a partition is opened. This method establishes the database
     * connection and prepares the SQL statement for batch inserts.
     *
     * @param partitionId A unique id for the partition.
     * @param epochId     A unique id for the epoch.
     * @return true if the connection was successfully opened; false otherwise.
     */
    @Override
    public boolean open(final long partitionId, final long epochId) {
        try {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.prepareStatement(INSERT_QUERY);
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to open database connection for partition {}", partitionId, e);
            return false;
        }
    }

    /**
     * Called for each record in the partition. This method sets the parameters
     * on the prepared statement and executes the insert.
     *
     * @param value The AdEvent record to process.
     */
    @Override
    public void process(final AdEvent value) {
        try {
            statement.setString(EVENT_ID_COL, value.getEventId());
            statement.setString(AD_CREATIVE_ID_COL, value.getAdCreativeId());
            statement.setString(USER_ID_COL, value.getUserId());
            statement.setString(EVENT_TYPE_COL, value.getEventType());
            statement.setTimestamp(EVENT_TIMESTAMP_COL, Timestamp.valueOf(value.getTimestamp()));
            statement.setTimestamp(PROCESSING_TIMESTAMP_COL, new Timestamp(System.currentTimeMillis()));
            statement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("Failed to process ad event with ID: {}", value.getEventId(), e);
        }
    }

    /**
     * Called when the processing of a partition is complete. This method closes
     * the database connection and statement resources.
     *
     * @param errorOrNull The error that caused the processing to stop, or null if it was stopped
     *                    gracefully.
     */
    @Override
    public void close(final Throwable errorOrNull) {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to close database connection", e);
        }
    }
}