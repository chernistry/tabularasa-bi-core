package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class SparkConfig {

    static {
        // Настройки для безопасного запуска Spark на Java 17+
        System.setProperty("hadoop.home.dir", "/");
        System.setProperty("java.security.manager", "allow");
    }

    @Value("${spark.app.name:TabularasaBI-Q1}")
    private String appName;

    @Value("${spark.master:local[*]}")
    private String master;

    @Value("${spark.sql.streaming.checkpointLocation:/tmp/spark-checkpoints/q1_ad_stream}")
    private String checkpointLocation;

    @Bean(destroyMethod = "close")
    @Lazy
    public SparkSession sparkSession() {
        try {
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

            // Устанавливаем временную зону для сессии
            session.conf().set("spark.sql.session.timeZone", "UTC");
            
            return session;
        } catch (Exception e) {
            System.err.println("Ошибка при создании SparkSession: " + e.getMessage());
            e.printStackTrace();
            // Возвращаем null вместо создания минимальной локальной сессии
            // Приложение должно обрабатывать возможное отсутствие Spark
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
}