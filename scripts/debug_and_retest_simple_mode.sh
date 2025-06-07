#!/bin/bash

# Script to rebuild, restart, and help test the Q1 Simple Processor

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT_DIR="$( cd "${SCRIPT_DIR}/.." &> /dev/null && pwd )"
Q1_MODULE_DIR="${PROJECT_ROOT_DIR}/q1_realtime_stream_processing"
# Corrected JAR name to match the executable Spring Boot JAR
Q1_JAR_NAME="q1_realtime_stream_processing-0.0.1-SNAPSHOT-spring-boot.jar" 
Q1_JAR_PATH="${Q1_MODULE_DIR}/target/${Q1_JAR_NAME}"
APP_LOG_DIR="${PROJECT_ROOT_DIR}/logs"

# ... existing code ... 