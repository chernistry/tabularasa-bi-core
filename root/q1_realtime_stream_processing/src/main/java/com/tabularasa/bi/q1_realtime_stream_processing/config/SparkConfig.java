package com.tabularasa.bi.q1_realtime_stream_processing.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator.class.getName())
                .set("spark.ui.enabled", "false")
                .set("spark.metrics.conf", "classpath:metrics.properties");
    }
    
    @Bean
    public JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        try {
            return new JavaSparkContext(sparkConf);
        } catch (Exception e) {
            log.error("Failed to create JavaSparkContext", e);
            throw e;
        }
    }

    @Bean
    public SparkSession sparkSession(SparkConf sparkConf) {
        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }
}
