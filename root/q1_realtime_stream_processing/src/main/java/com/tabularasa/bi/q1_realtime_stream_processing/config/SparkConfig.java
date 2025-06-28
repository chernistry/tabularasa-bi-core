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

    @Value("${spark.driver.memory:1g}")
    private String driverMemory;

    @Value("${spark.executor.memory:1g}")
    private String executorMemory;

    @Value("${spark.executor.cores:2}")
    private int executorCores;

    @Value("${spark.default.parallelism:16}")
    private int defaultParallelism;

    @Value("${spark.sql.shuffle.partitions:16}")
    private int shufflePartitions;

    @Bean
    @Primary
    public SparkSession sparkSession() {
        log.info("Creating SparkSession with app name: {}, master: {}, driverMemory: {}, executorMemory: {}", 
                appName, master, driverMemory, executorMemory);
                
        return SparkSession.builder()
                .appName(appName)
                .master(master)
                // Основные параметры сериализации
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator.class.getName())
                .config("spark.kryo.unsafe", "true")
                
                // Настройки ресурсов
                .config("spark.driver.memory", driverMemory)
                .config("spark.executor.memory", executorMemory)
                .config("spark.executor.cores", executorCores)
                .config("spark.cores.max", executorCores * 2)
                
                // Настройки параллелизма
                .config("spark.default.parallelism", defaultParallelism)
                .config("spark.sql.shuffle.partitions", shufflePartitions)
                
                // Scheduler settings
                .config("spark.scheduler.mode", "FAIR")
                .config("spark.scheduler.allocation.file", "classpath:fairscheduler.xml")
                
                // Checkpoint
                .config("spark.streaming.unpersist", "true")
                .config("spark.streaming.kafka.maxRatePerPartition", "10000")
                .config("spark.streaming.backpressure.enabled", "true")
                
                // Network optimization
                .config("spark.network.timeout", "800s")
                .config("spark.executor.heartbeatInterval", "60s")
                
                // Memory optimization
                .config("spark.memory.fraction", "0.8")
                .config("spark.memory.storageFraction", "0.3")
                
                // Dynamic allocation
                .config("spark.dynamicAllocation.enabled", "true")
                .config("spark.dynamicAllocation.initialExecutors", "1")
                .config("spark.dynamicAllocation.minExecutors", "1")
                .config("spark.dynamicAllocation.maxExecutors", "4")
                .config("spark.dynamicAllocation.schedulerBacklogTimeout", "30s")
                
                // UI и метрики (отключаем для оптимизации)
                .config("spark.ui.enabled", "false")
                .config("spark.metrics.staticSources.enabled", "false")
                
                // Deployment-specific settings
                .config("spark.submit.deployMode", "client")
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
