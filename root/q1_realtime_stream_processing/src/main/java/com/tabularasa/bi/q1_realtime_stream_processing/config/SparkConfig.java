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

    @Value("${spark.master:local[2]}")
    private String sparkMaster;

    @Value("${spark.driver.memory:1g}")
    private String driverMemory;

    @Value("${spark.executor.memory:1g}")
    private String executorMemory;

    @Value("${spark.executor.cores:1}")
    private int executorCores;

    @Value("${spark.default.parallelism:2}")
    private int defaultParallelism;

    @Value("${spark.sql.shuffle.partitions:2}")
    private int shufflePartitions;

    @Bean
    @Primary
    public SparkSession sparkSession() {
        // Определяем, запускаемся ли в локальном режиме или в Docker-контейнере
        boolean isLocalMaster = sparkMaster.startsWith("local") || 
                               sparkMaster.equals("spark://localhost:7077") || 
                               sparkMaster.equals("spark://spark-master:7077");
        
        log.info("Creating SparkSession with app name: {}, master: {}, driverMemory: {}, executorMemory: {}", 
                appName, sparkMaster, driverMemory, executorMemory);
        
        // Всегда используем локальный режим для отладки проблем с подключением
        String actualMaster = "local[2]";
        
        if (isLocalMaster) {
            log.info("Using local mode with 2 threads to avoid connectivity issues");
        } else {
            log.info("Configured master is {}, but using local mode for debugging", sparkMaster);
        }
        
        SparkConf conf = new SparkConf()
            .setAppName(appName)
            .setMaster(actualMaster)
            // Основные параметры сериализации
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrationRequired", "false")
            
            // Настройки ресурсов - уменьшаем для локального режима
            .set("spark.driver.memory", driverMemory)
            .set("spark.executor.memory", executorMemory)
            .set("spark.executor.cores", String.valueOf(executorCores))
            
            // Настройки параллелизма - уменьшаем для стабильности
            .set("spark.default.parallelism", String.valueOf(defaultParallelism))
            .set("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
            
            // Scheduler settings
            .set("spark.scheduler.mode", "FAIR")
            
            // Checkpoint
            .set("spark.streaming.unpersist", "true")
            .set("spark.streaming.kafka.maxRatePerPartition", "100")
            .set("spark.streaming.backpressure.enabled", "true")
            
            // Network optimization - увеличиваем таймауты
            .set("spark.network.timeout", "600s")
            .set("spark.executor.heartbeatInterval", "60s")
            
            // Memory optimization
            .set("spark.memory.fraction", "0.6")
            .set("spark.memory.storageFraction", "0.2")
            
            // Отключаем dynamic allocation для локального режима
            .set("spark.dynamicAllocation.enabled", "false")
            
            // Отключаем UI для решения проблемы с конфликтами servlet API
            .set("spark.ui.enabled", "false")
            .set("spark.ui.showConsoleProgress", "true")
            
            // Установка уровня логирования через конфигурацию вместо прямого вызова
            .set("spark.log.level", "INFO");

        return SparkSession.builder()
            .config(conf)
            .getOrCreate();
    }
    
    @Bean
    public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
        try {
            log.info("Creating JavaSparkContext from SparkSession");
            // Используем существующий SparkContext из SparkSession
            JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
            
            // Удаляем прямой вызов setLogLevel, который вызывает ClassCastException
            // между SLF4J и Log4j2 контекстами
            
            return jsc;
        } catch (Exception e) {
            log.error("Failed to create JavaSparkContext", e);
            throw e;
        }
    }
}
