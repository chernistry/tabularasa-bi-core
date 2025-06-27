package com.tabularasa.bi.q1_realtime_stream_processing.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("spark")
public class SparkConfig {

    // Only for local/test profile. For cluster, use spark-submit and external config.
    @Value("${spark.app.name:Q1RealtimeStreamProcessing}")
    private String appName;

    @Value("${spark.master:local[*]}")
    private String master;

    @Bean
    @Profile("local")
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(appName)
                .master(master)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator.class.getName())
                .getOrCreate();
    }
}
