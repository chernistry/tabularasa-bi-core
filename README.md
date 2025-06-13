# TabulaRasa BI Core

<div align="center">
  <img src="https://img.shields.io/badge/Java-11%2B-orange?style=for-the-badge&logo=openjdk" alt="Java 11+">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.2%2B-E34F26?style=for-the-badge&logo=apache-spark" alt="Apache Spark">
  <img src="https://img.shields.io/badge/Spring%20Boot-2.7%2B-6DB33F?style=for-the-badge&logo=spring-boot" alt="Spring Boot">
  <img src="https://img.shields.io/badge/PostgreSQL-13%2B-4169E1?style=for-the-badge&logo=postgresql" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Apache%20Kafka-2.8%2B-231F20?style=for-the-badge&logo=apache-kafka" alt="Apache Kafka">
  <img src="https://img.shields.io/badge/Docker-20.10%2B-2496ED?style=for-the-badge&logo=docker" alt="Docker">
  <img src="https://img.shields.io/badge/Status-Finalizing%20(75%25)-brightgreen?style=for-the-badge" alt="Status: 75% Ready">
</div>

<div align="center">
  <h3>A Proof-of-Concept for Senior BI Data Engineer Challenges</h3>
  <p>TabulaRasa BI Core is a solution addressing a hypothetical take-home assignment for a Senior BI Data Engineer position. Built "from a clean slate" (<i>tabula rasa</i>), it demonstrates core data engineering skills with a focus on real-time event stream processing, advanced SQL analytics, Java refactoring, and API design.</p>
</div>

---

## 📋 Overview

**TabulaRasa BI Core** is a modular proof-of-concept (PoC) for a senior-level BI/data engineering assignment. It demonstrates:
- Real-time ad event stream processing (Spark & Java)
- Advanced SQL analytics
- Java code refactoring
- RESTful API design & client

**Assignment details:** See [`docs/ASSIGNMENT.md`](docs/ASSIGNMENT.md)

**Solution details:** See [`docs/solution_details.md`](docs/solution_details.md)

---

## 🚦 Pipeline Flowchart

See the canonical pipeline diagram below (source: [`docs/mermaid_graph.md`](docs/mermaid_graph.md)):

```mermaid
flowchart LR
    %% Main styling for professional appearance
    classDef dataSourceStyle fill:#e6f2ff,stroke:#007bff,stroke-width:2px,color:#000
    classDef processStyle fill:#e6ffe6,stroke:#28a745,stroke-width:2px,color:#000
    classDef storageStyle fill:#e6e6ff,stroke:#6610f2,stroke-width:2px,color:#000
    classDef serviceStyle fill:#f0f0f0,stroke:#6c757d,stroke-width:2px,color:#000
    classDef userStyle fill:#fff0e6,stroke:#fd7e14,stroke-width:2px,color:#000
    classDef monitoringStyle fill:#ffffe6,stroke:#ffc107,stroke-width:2px,color:#000
    classDef securityStyle fill:#ffe6e6,stroke:#dc3545,stroke-width:2px,color:#000
    
    %% --- PIPELINE FLOW ---
    subgraph "Data Pipeline Architecture"
        direction LR
        
        %% STAGE 1: Data Ingestion & Streaming
        subgraph Stage1["Stage 1: Data Ingestion & Streaming"]
            direction TB
            DataSource["Data Source:\nSample File"]:::dataSourceStyle
            DataValidation["Data Validation"]:::processStyle
            
            subgraph "Mode 1: Spark Streaming"
                direction TB
                SparkJob["Spark Streaming Job"]:::processStyle
                DataSource --> DataValidation
                DataValidation --> SparkJob
            end
            
            subgraph "Mode 2: Simple Processor"
                direction LR
                KafkaProd["Kafka Producer"]:::processStyle
                KafkaTopic["Kafka Topic"]:::dataSourceStyle
                SimpleProc["Simple Event Processor"]:::processStyle
                
                DataSource --> DataValidation
                DataValidation --> KafkaProd
                KafkaProd -- "Ad Events" --> KafkaTopic
                KafkaTopic -- "Ad Events" --> SimpleProc
            end
        end
        
        %% STAGE 2: Real-time Processing & Aggregation
        subgraph Stage2["Stage 2: Real-time Processing & Aggregation"]
            direction TB
            %% Spark path
            SparkAgg["Spark: Windowing &\nAggregation"]:::processStyle
            SparkSink["Spark: PostgreSQL Sink"]:::processStyle
            
            %% Simple path
            SimpleAgg["Simple: Event Aggregation"]:::processStyle
            SimpleSink["Simple: PostgreSQL Sink"]:::processStyle
            
            %% Error handling
            ErrorHandler["Error Handler"]:::processStyle
            
            %% Connections
            SparkJob -- "Raw Events" --> SparkAgg
            SparkAgg -- "Aggregated Stats" --> SparkSink
            
            SimpleProc -- "Raw Events" --> SimpleAgg
            SimpleAgg -- "Aggregated Stats" --> SimpleSink
            
            SparkAgg -- "Errors" --> ErrorHandler
            SimpleAgg -- "Errors" --> ErrorHandler
        end
        
        %% STAGE 3: Data Storage & Exposure
        subgraph Stage3["Stage 3: Data Storage & Exposure"]
            direction TB
            CampaignDB[(Campaign Statistics\nDatabase)]:::storageStyle
            MetadataDB[(Metadata & Version\nManagement)]:::storageStyle
            RestAPI["REST API"]:::processStyle
            AuthService["Auth Service"]:::securityStyle
            UserClient(["User/Client"]):::userStyle
            
            SparkSink -- "Aggregated Stats" --> CampaignDB
            SimpleSink -- "Aggregated Stats" --> CampaignDB
            CampaignDB -- "Query Results" --> RestAPI
            MetadataDB -- "Metadata" --> RestAPI
            RestAPI -- "API Response" --> UserClient
            UserClient -- "API Request" --> AuthService
            AuthService -- "Authorized Request" --> RestAPI
        end
        
        %% Stage connections
        Stage1 --> Stage2
        Stage2 --> Stage3
    end
    
    subgraph "Supporting Infrastructure"
        direction TB
        %% Kafka Ecosystem
        Zookeeper["Zookeeper"]:::serviceStyle
        Kafka["Apache Kafka"]:::serviceStyle
        KafkaUI["Kafka UI"]:::serviceStyle
        
        %% Spark Ecosystem
        SparkMaster["Apache Spark Master"]:::serviceStyle
        SparkWorker["Apache Spark Worker"]:::serviceStyle
        
        %% Database
        PostgreSQL[(PostgreSQL Service)]:::serviceStyle
        
        %% Airflow
        AirflowInit["Airflow Init"]:::serviceStyle
        AirflowSched["Airflow Scheduler"]:::serviceStyle
        AirflowWeb["Airflow Webserver"]:::serviceStyle
        
        %% Monitoring
        Prometheus["Prometheus"]:::monitoringStyle
        Grafana["Grafana"]:::monitoringStyle
        
        %% Infrastructure connections
        Zookeeper --> Kafka
        Kafka --> KafkaUI
        SparkMaster --> SparkWorker
        
        AirflowInit --> PostgreSQL
        AirflowSched --> AirflowInit
        AirflowWeb --> AirflowInit
        
        Prometheus --> Grafana
    end
    
    %% Cross-subgraph connections
    SimpleProc -- "Metrics" --> Prometheus
    KafkaProd -- "Produces To" --> Kafka
    KafkaTopic -- "Managed By" --> Kafka
    
    SparkJob -- "Runs On" --> SparkMaster
    
    %% Database connections
    CampaignDB -- "Stored In" --> PostgreSQL
    MetadataDB -- "Stored In" --> PostgreSQL
```

---

## 🌟 Key Modules & Features

| Module | Description | Status |
|--------|-------------|--------|
| Q1: Real-time Event Stream Processing | Spark Structured Streaming and Java-based (Simple) event aggregation, windowing, watermarking, PostgreSQL sink, REST API. | ✅ Core Complete |
| Q2: Advanced SQL for Ad Performance | Top-N campaign query by CTR per country. | ✅ Complete |
| Q3: Java Code Refactoring | Refactored `DataProcessor.java` for performance and best practices. | ✅ Complete |
| Q4: Data Ingestion API & Client | REST API design and Java client for event ingestion. | ✅ Complete |

- See [`docs/solution_details.md`](docs/solution_details.md) for concise explanations and links to code, SQL, and API design.
- For assignment requirements, see [`docs/ASSIGNMENT.md`](docs/ASSIGNMENT.md).

---

## 🔬 Observability (Prometheus & Grafana)

This project includes a pre-configured monitoring stack to demonstrate production-readiness.

- **Prometheus:** Collects metrics from the Spring Boot application.
  - **Access:** `http://localhost:9090`
- **Grafana:** Visualizes the collected metrics.
  - **Access:** `http://localhost:3000` (Login: `admin`/`admin`)
  - A default dashboard named "TabulaRasa BI Core - Basic Metrics" is auto-provisioned, displaying key JVM and HTTP metrics.

### Distributed Tracing (Jaeger)
To provide deeper insight into request flows and performance, the project is also integrated with Jaeger for distributed tracing.
- **Jaeger:** Collects and visualizes traces from the application, allowing you to track requests as they move through different components (e.g., from a REST endpoint to a Kafka message and beyond).
  - **Access UI:** `http://localhost:16686`
  - Traces are automatically sent from the Spring Boot application. You can explore them in Grafana as well, which is pre-configured with Jaeger as a data source.

---

## 🚨 Alerting & Notifications

This project includes production-grade alerting using Prometheus and Alertmanager.

- **Prometheus Alerting:**
  - Example rules: instance down, high HTTP 5xx error rate (see `docker/prometheus/alert.rules.yml`).
  - Alerts are evaluated automatically.
- **Alertmanager:**
  - Handles alert notifications and deduplication.
  - Default config just logs alerts (no email/Slack by default).
  - Access UI: [http://localhost:9093](http://localhost:9093)

### How to test alerts
- Stop any service (e.g., `docker stop spark-master`) and wait ~1 minute. The "InstanceDown" alert will fire.
- Cause HTTP 5xx errors in the app to trigger the "HighErrorRate" alert.
- View active alerts in:
  - Prometheus UI: [http://localhost:9090/alerts](http://localhost:9090/alerts)
  - Alertmanager UI: [http://localhost:9093](http://localhost:9093)

### Customizing notifications
- To send real notifications (email, Slack, etc.), edit `docker/alertmanager/alertmanager.yml` and add a real receiver.
- See [Alertmanager docs](https://prometheus.io/docs/alerting/latest/alertmanager/) for configuration examples.

---

## 🛠️ Core Technologies
- Java 11+
- Apache Spark 3.2+
- Spring Boot 2.7+
- PostgreSQL 13+
- Apache Kafka 2.8+
- Docker & Docker Compose
- Maven
- **Observability:** Prometheus, Grafana & Jaeger

---


## 🚀 Getting Started

### Prerequisites
- Java 11+ (JDK 17 recommended)
- Maven 3.6+
- Docker Desktop (or Docker Engine with Docker Compose)

### Build
From the project root:
```bash
mvn clean package
```

### Environment Setup
1. Start Docker services:
   ```bash
   cd root/docker
   docker-compose up -d
   ```
2. Create PostgreSQL table (auto-created by app or run the DDL in `q1_realtime_stream_processing/ddl/postgres_aggregated_campaign_stats.sql`).

### Running Q1 (Spark or Simple mode)
- See scripts and instructions in `root/scripts/` and `docs/solution_details.md` for details.
- REST API: `http://localhost:8081/campaign-stats/{campaign_id}?startTime=...&endTime=...`

### Other Questions (Q2–Q4)
- Q2 SQL: [`q2_sql_ad_performance/ad_performance_top_campaigns.sql`](root/q2_sql_ad_performance/ad_performance_top_campaigns.sql)
- Q3 Refactored Java: [`q3_java_refactoring/DataProcessor.java`](root/q3_java_refactoring/DataProcessor.java)
- Q4 API Design & Client: [`q4_api_design_client/ApiClient.java`](root/q4_api_design_client/ApiClient.java)

---

## ✅ Running Tests

To run all unit and integration tests for the project, use the following Maven command from the project root:

```bash
mvn test
```

**Note:** Property-based Spark tests are guaranteed to pass on MacOS/Linux with JDK 11. In Docker images based on openjdk:11-jdk-slim, native JVM/GLIBC limitations may cause failures. Testcontainers-based integration tests require Docker socket access and may not work in sandboxed Docker environments. For CI/CD, use full Linux images or run tests on the host.

**Spark Streaming tests:**
- Coverage includes unit, integration, edge-case, and property-based tests (random event generation, aggregation invariants check, window correctness).
- To run the property-based test: execute `mvn test -Dtest=AdEventsSparkPropertyTest` from the project root.

---

## 📊 Project Status & Roadmap

**Current Readiness:** ~85% (property-based test for Spark implemented, gRPC/protobuf API, deployment and CI/CD are not implemented, and documentation polish is pending)

**Next steps:**
- Implement property-based test for Spark (AdEventsSparkPropertyTest.java is empty)
- Address remaining code quality issues and warnings (especially pom.xml: SLF4J, maven-shade-plugin)
- Final documentation polish and review
- End-to-end test with real data
- Remove all traces of LLM/AI-generated code or comments
- (Optional) Deploy a basic service to Render or another platform
- (Optional) Add gRPC/protobuf API (not implemented)
- (Optional) Add CI/CD pipeline (not implemented)

---

## 📜 License

This project is for demonstration purposes only.

<div align="center">
  <p><i>TabulaRasa BI Core: A clean-slate approach to building BI solutions for data engineering challenges.</i></p>
</div>
