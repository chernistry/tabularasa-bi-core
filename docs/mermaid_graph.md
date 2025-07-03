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
        
        %% Monitoring
        subgraph Monitoring["Monitoring & Alerting"]
            direction TB
            Metrics["Metrics Collector"]:::monitoringStyle
            Alerts["Alert Manager"]:::monitoringStyle
            Dashboard["Monitoring Dashboard"]:::monitoringStyle
            
            SparkJob -- "Performance Metrics" --> Metrics
            KafkaProd -- "Performance Metrics" --> Metrics
            SimpleProc -- "Performance Metrics" --> Metrics
            SparkAgg -- "Performance Metrics" --> Metrics
            SimpleAgg -- "Performance Metrics" --> Metrics
            RestAPI -- "API Metrics" --> Metrics
            
            Metrics --> Alerts
            Alerts --> Dashboard
            ErrorHandler -- "Error Events" --> Alerts
        end
    end
    
    %% --- INFRASTRUCTURE ---
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
        
        %% Airflow (Orchestration)
        subgraph Airflow["Airflow Orchestration"]
            direction TB
            AirflowInit["Airflow Init"]:::serviceStyle
            AirflowSched["Airflow Scheduler"]:::serviceStyle
            AirflowWeb["Airflow Webserver"]:::serviceStyle
            AirflowDAGs["Pipeline DAGs"]:::serviceStyle
        end
        
        %% Infrastructure connections
        Zookeeper --> Kafka
        Kafka --> KafkaUI
        SparkMaster --> SparkWorker
        
        AirflowInit --> PostgreSQL
        AirflowSched --> AirflowInit
        AirflowWeb --> AirflowInit
        AirflowDAGs --> AirflowSched
    end
    
    %% --- CONNECTIONS BETWEEN PIPELINE AND INFRASTRUCTURE ---
    %% Kafka connections
    KafkaProd -- "Produces To" --> Kafka
    KafkaTopic -- "Managed By" --> Kafka
    
    %% Spark connections
    SparkJob -- "Runs On" --> SparkMaster
    SparkAgg -- "Runs On" --> SparkWorker
    
    %% Database connections
    CampaignDB -- "Stored In" --> PostgreSQL
    MetadataDB -- "Stored In" --> PostgreSQL
    
    %% Orchestration connections
    AirflowDAGs -- "Orchestrates" --> SparkJob
    AirflowDAGs -- "Orchestrates" --> KafkaProd
    AirflowDAGs -- "Monitors" --> CampaignDB