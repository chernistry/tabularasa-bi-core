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
import org.springframework.beans.factory.DisposableBean;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

@Configuration
@Profile("spark")
@Slf4j
public class SparkConfig implements DisposableBean {

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
    
    @Value("${spark.streaming.checkpoint-location:/tmp/spark_checkpoints}")
    private String checkpointLocation;
    
    private SparkSession sparkSession;
    private JavaSparkContext javaSparkContext;

    @Bean
    @Primary
    public SparkSession sparkSession() {
        // Determine if running in local mode or in a Docker container
        boolean isLocalMaster = sparkMaster.startsWith("local") || 
                               sparkMaster.equals("spark://localhost:7077") || 
                               sparkMaster.equals("spark://spark-master:7077");
        
        log.info("Creating SparkSession with app name: {}, master: {}, driverMemory: {}, executorMemory: {}", 
                appName, sparkMaster, driverMemory, executorMemory);
        
        // Always use local mode for debugging connectivity issues
        String actualMaster = "local[2]";
        
        if (isLocalMaster) {
            log.info("Using local mode with 2 threads to avoid connectivity issues");
        } else {
            log.info("Configured master is {}, but using local mode for debugging", sparkMaster);
        }
        
        // Clean up the checkpoint directory before starting
        try {
            cleanupCheckpointDirectory();
        } catch (Exception e) {
            log.warn("Failed to cleanup checkpoint directory: {}", e.getMessage());
        }
        
        SparkConf conf = new SparkConf()
            .setAppName(appName)
            .setMaster(actualMaster)
            // Main serialization parameters
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrationRequired", "false")
            
            // Resource settings - reduce for local mode
            .set("spark.driver.memory", driverMemory)
            .set("spark.executor.memory", executorMemory)
            .set("spark.executor.cores", String.valueOf(executorCores))
            
            // Parallelism settings - reduce for stability
            .set("spark.default.parallelism", String.valueOf(defaultParallelism))
            .set("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
            
            // Scheduler settings
            .set("spark.scheduler.mode", "FAIR")
            
            // Checkpoint
            .set("spark.streaming.unpersist", "true")
            .set("spark.streaming.kafka.maxRatePerPartition", "100")
            .set("spark.streaming.backpressure.enabled", "true")
            
            // Network optimization - increase timeouts
            .set("spark.network.timeout", "600s")
            .set("spark.executor.heartbeatInterval", "60s")
            
            // Memory optimization
            .set("spark.memory.fraction", "0.6")
            .set("spark.memory.storageFraction", "0.2")
            
            // Disable dynamic allocation for local mode
            .set("spark.dynamicAllocation.enabled", "false")
            
            // Disable UI to resolve servlet API conflict issues
            .set("spark.ui.enabled", "false")
            .set("spark.ui.showConsoleProgress", "true")
            
            // Set proper handling for graceful shutdown
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .set("spark.cleaner.periodicGC.interval", "1min")
            
            // Improve error management for seamless shutdown
            .set("spark.task.maxFailures", "3")
            .set("spark.rpc.message.maxSize", "128")
            .set("spark.rpc.io.connectionTimeout", "120s")
            .set("spark.rpc.lookupTimeout", "120s")
            .set("spark.rpc.numRetries", "5")
            .set("spark.rpc.retry.wait", "5s")
            
            // Set logging level through configuration instead of direct call
            .set("spark.log.level", "INFO");

        this.sparkSession = SparkSession.builder()
            .config(conf)
            .getOrCreate();
            
        return sparkSession;
    }
    
    @Bean
    public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
        try {
            log.info("Creating JavaSparkContext from SparkSession");
            // Use existing SparkContext from SparkSession
            this.javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
            
            // Remove direct call to setLogLevel which causes ClassCastException
            // between SLF4J and Log4j2 contexts
            
            return javaSparkContext;
        } catch (Exception e) {
            log.error("Failed to create JavaSparkContext", e);
            throw e;
        }
    }
    
    private void cleanupCheckpointDirectory() throws Exception {
        Path checkpointPath = Paths.get(checkpointLocation);
        if (Files.exists(checkpointPath)) {
            log.info("Cleaning up checkpoint directory: {}", checkpointPath);
            Files.walk(checkpointPath)
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(File::delete);
        }
        
        Files.createDirectories(checkpointPath);
        log.info("Checkpoint directory recreated: {}", checkpointPath);
    }
    
    @Override
    public void destroy() throws Exception {
        log.info("Shutting down Spark resources");
        if (javaSparkContext != null && !javaSparkContext.sc().isStopped()) {
            try {
                log.info("Stopping JavaSparkContext");
                javaSparkContext.close();
            } catch (Exception e) {
                log.warn("Error stopping JavaSparkContext: {}", e.getMessage());
            }
        }
        
        if (sparkSession != null && !sparkSession.sparkContext().isStopped()) {
            try {
                log.info("Stopping SparkSession");
                sparkSession.stop();
            } catch (Exception e) {
                log.warn("Error stopping SparkSession: {}", e.getMessage());
            }
        }
    }
}
