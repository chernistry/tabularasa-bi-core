package com.tabularasa.bi.q1_realtime_stream_processing.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

@Configuration
@Profile("spark")
@Slf4j
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String master;
    
    @Value("${spark.metrics.conf:classpath:metrics.properties}")
    private String metricsConfig;

    @Bean
    public SparkConf sparkConf() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator.class.getName())
                .set("spark.ui.enabled", "false");
        
        // Handle metrics configuration
        if (metricsConfig != null && metricsConfig.startsWith("classpath:")) {
            String resourcePath = metricsConfig.substring("classpath:".length());
            Resource resource = new ClassPathResource(resourcePath);
            
            if (resource.exists()) {
                try {
                    // Создаем временный файл для копирования содержимого из JAR
                    Path tempFile = Files.createTempFile("spark-metrics-", ".properties");
                    File metricsFile = tempFile.toFile();
                    metricsFile.deleteOnExit();
                    
                    // Копируем содержимое из classpath в временный файл
                    try (InputStream is = resource.getInputStream();
                         FileOutputStream fos = new FileOutputStream(metricsFile)) {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            fos.write(buffer, 0, bytesRead);
                        }
                    }
                    
                    String absolutePath = metricsFile.getAbsolutePath();
                    conf.set("spark.metrics.conf", absolutePath);
                    log.info("Using metrics configuration from temporary file: {}", absolutePath);
                } catch (IOException e) {
                    log.warn("Could not create temporary metrics file. Using default metrics configuration.", e);
                    // Отключаем метрики при ошибке
                    conf.set("spark.metrics.staticSources.enabled", "false");
                }
            } else {
                log.warn("Metrics configuration file not found: {}. Using default metrics configuration.", resourcePath);
                // Disable metrics servlet to avoid errors if the file doesn't exist
                conf.set("spark.metrics.staticSources.enabled", "false");
            }
        }
        
        return conf;
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
}
