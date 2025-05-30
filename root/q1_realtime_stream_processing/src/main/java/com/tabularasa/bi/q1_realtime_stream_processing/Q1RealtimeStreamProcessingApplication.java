package com.tabularasa.bi.q1_realtime_stream_processing;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
public class Q1RealtimeStreamProcessingApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(Q1RealtimeStreamProcessingApplication.class);

    public static void main(String[] args) {
        try {
            // Установка системных свойств перед запуском Spring Boot
            System.setProperty("hadoop.home.dir", "/");
            System.setProperty("java.security.manager", "allow");
            
            SpringApplication.run(Q1RealtimeStreamProcessingApplication.class, args);
        } catch (Exception e) {
            logger.error("Критическая ошибка при запуске приложения", e);
            System.exit(1);
        }
    }

    /**
     * Проверяет доступность Spark и выполняет соответствующие действия
     */
    @Bean
    public CommandLineRunner checkSparkAvailability(SparkSession sparkSession) {
        return args -> {
            if (sparkSession == null) {
                logger.warn("=====================================");
                logger.warn("ВНИМАНИЕ: SparkSession не инициализирована!");
                logger.warn("Потоковая обработка Spark не будет работать!");
                logger.warn("Будут доступны только REST API и Kafka Producer функции.");
                logger.warn("=====================================");
            } else {
                logger.info("SparkSession успешно инициализирована.");
                logger.info("Spark версия: {}", sparkSession.version());
                logger.info("Spark работает в режиме: {}", sparkSession.sparkContext().master());
            }
        };
    }
} 