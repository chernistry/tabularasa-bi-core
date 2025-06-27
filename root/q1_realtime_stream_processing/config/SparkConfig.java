package com.example.q1_realtime_stream_processing.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
public class SparkConfig {

    @Value("${app.name}")
    private String appName;

    @Value("${spark.master}")
    private String master;

    @Bean
    @Profile("spark")
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(appName)
                .master(master)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.streaming.backpressure.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.kryo.registrationRequired", "false")
                .config("spark.ui.showConsoleProgress", "true")
                .getOrCreate();
    }
} 