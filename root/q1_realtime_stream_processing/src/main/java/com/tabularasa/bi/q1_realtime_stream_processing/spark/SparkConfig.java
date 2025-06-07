package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Qualifier;
import java.util.Map;

/**
 * Spark configuration, only active when 'spark' profile is enabled.
 */
@Profile("spark")
@Configuration
public class SparkConfig {

    @Value("${spark.app.name:TabularasaBI-Q1}")
    private String appName;

    @Value("${spark.master:local[*]}")
    private String master;

    @Value("${spark.sql.streaming.checkpointLocation:/tmp/spark-checkpoints/q1_ad_stream}")
    private String checkpointLocation;

    @Value("${kafka.topic.ad_events:ad_events_topic}")
    private String adEventsTopicForSparkStreamer;

    @Bean(destroyMethod = "close")
    @Lazy
    public SparkSession sparkSession() {
        try {
            // Safe Spark startup for Java 17+
            System.setProperty("hadoop.home.dir", "/");
            SparkConf conf = new SparkConf()
                    .setAppName(appName)
                    .setMaster(master)
                    .set("spark.sql.streaming.checkpointLocation", checkpointLocation)
                    .set("spark.driver.allowMultipleContexts", "true")
                    .set("spark.unsafe.exceptionOnMemoryLeak", "false")
                    .set("spark.io.compression.codec", "snappy")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
                    .set("spark.shuffle.manager", "sort");
            SparkSession session = SparkSession.builder()
                    .config(conf)
                    .getOrCreate();
            session.conf().set("spark.sql.session.timeZone", "UTC");
            return session;
        } catch (Exception e) {
            System.err.println("Error creating SparkSession: " + e.getMessage());
            e.printStackTrace();
            // Return null if Spark cannot be started; application must handle this case
            return null;
        }
    }

    @Bean
    @Lazy
    public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
        if (sparkSession == null) {
            return null;
        }
        return new JavaSparkContext(sparkSession.sparkContext());
    }

    /**
     * AdEventSparkStreamer bean definition.
     * Uses SparkSession, Kafka consumer properties, and topic value.
     */
    @Bean
    public AdEventSparkStreamer adEventSparkStreamer(
            SparkSession sparkSession, 
            @Qualifier("sparkKafkaConsumerProperties") Map<String, String> kafkaConsumerProperties
            ) {
        return new AdEventSparkStreamer(sparkSession, kafkaConsumerProperties, adEventsTopicForSparkStreamer);
    }
}