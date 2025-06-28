package com.tabularasa.bi.q1_realtime_stream_processing.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Profile("spark")
@Slf4j
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String master;

    @Bean
    @Primary
    public SparkSession sparkSession() {
        log.info("Creating SparkSession with app name: {}, master: {}", appName, master);
        return SparkSession.builder()
                .appName(appName)
                .master(master)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator.class.getName())
                .config("spark.ui.enabled", "false")
                .config("spark.metrics.staticSources.enabled", "false")
                .getOrCreate();
    }
    
    @Bean
    public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
        try {
            log.info("Creating JavaSparkContext from SparkSession");
            // Используем существующий SparkContext из SparkSession
            return new JavaSparkContext(sparkSession.sparkContext());
        } catch (Exception e) {
            log.error("Failed to create JavaSparkContext", e);
            throw e;
        }
    }
}
