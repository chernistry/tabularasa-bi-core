#!/bin/bash

# Set custom Java options for Spark to handle serialization issues
export SPARK_JAVA_OPTS="$SPARK_JAVA_OPTS -Dsun.io.serialization.extendedDebugInfo=true -Djava.io.serialization.strict=false -Djdk.serialFilter=allow -Dsun.serialization.validateClassSerialVersionUID=false" 