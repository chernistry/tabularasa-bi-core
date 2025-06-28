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

    @Value("${spark.master:local[*]}")
    private String sparkMaster;

    @Value("${spark.driver.memory:1g}")
    private String driverMemory;

    @Value("${spark.executor.memory:1g}")
    private String executorMemory;

    @Value("${spark.executor.cores:2}")
    private int executorCores;

    @Value("${spark.default.parallelism:4}")
    private int defaultParallelism;

    @Value("${spark.sql.shuffle.partitions:4}")
    private int shufflePartitions;

    @Bean
    @Primary
    public SparkSession sparkSession() {
        // Определяем, запускаемся ли в локальном режиме
        boolean isLocalMaster = sparkMaster.startsWith("local") || sparkMaster.equals("spark://localhost:7077");
        
        log.info("Creating SparkSession with app name: {}, master: {}, driverMemory: {}, executorMemory: {}", 
                appName, sparkMaster, driverMemory, executorMemory);
        
        // Выбираем оптимальные настройки в зависимости от режима
        String actualMaster = sparkMaster;
        if (sparkMaster.equals("spark://localhost:7077")) {
            log.info("Using local mode instead of localhost:7077 to avoid connectivity issues");
            actualMaster = "local[*]";
        }
        
        SparkConf conf = new SparkConf()
            .setAppName(appName)
            .setMaster(actualMaster)
            // Основные параметры сериализации
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator")
            .set("spark.kryo.registrationRequired", "false")
            
            // Настройки ресурсов
            .set("spark.driver.memory", driverMemory)
            .set("spark.executor.memory", executorMemory)
            .set("spark.executor.cores", String.valueOf(executorCores))
            
            // Настройки параллелизма
            .set("spark.default.parallelism", String.valueOf(defaultParallelism))
            .set("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
            
            // Scheduler settings
            .set("spark.scheduler.mode", "FAIR")
            
            // Checkpoint
            .set("spark.streaming.unpersist", "true")
            .set("spark.streaming.kafka.maxRatePerPartition", "10000")
            .set("spark.streaming.backpressure.enabled", "true")
            
            // Network optimization
            .set("spark.network.timeout", "300s")
            .set("spark.executor.heartbeatInterval", "30s")
            
            // Memory optimization
            .set("spark.memory.fraction", "0.8")
            .set("spark.memory.storageFraction", "0.3")
            
            // Отключаем dynamic allocation для локального режима
            .set("spark.dynamicAllocation.enabled", "false")
            
            // UI и метрики (отключаем для оптимизации)
            .set("spark.ui.enabled", "false")
            .set("spark.metrics.staticSources.enabled", "false");

        return SparkSession.builder()
            .config(conf)
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
