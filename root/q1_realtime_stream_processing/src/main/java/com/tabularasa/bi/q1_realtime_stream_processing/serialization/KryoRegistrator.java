package com.tabularasa.bi.q1_realtime_stream_processing.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AggregatedCampaignStats;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Custom Kryo registrator for Spark serialization.
 * Registers all classes that need to be serialized by Kryo.
 * Note: This class is only used when Kryo serialization is enabled.
 */
public class KryoRegistrator implements org.apache.spark.serializer.KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        // Register model classes
        kryo.register(AdEvent.class);
        kryo.register(AggregatedCampaignStats.class);
        
        // Register common Java classes
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.LinkedHashMap.class);
        kryo.register(java.util.HashSet.class);
        kryo.register(java.util.Date.class);
        kryo.register(java.sql.Timestamp.class);
        kryo.register(java.time.LocalDateTime.class);
        kryo.register(java.math.BigDecimal.class);
        kryo.register(String[].class);
        kryo.register(Object[].class);
        kryo.register(byte[].class);
        
        // Register arrays of primitive types
        kryo.register(int[].class);
        kryo.register(float[].class);
        kryo.register(double[].class);
        kryo.register(boolean[].class);
        kryo.register(char[].class);
        kryo.register(long[].class);
        
        // Register Scala collections with ordered registration for proxies first
        try {
            // Important: register these proxies BEFORE registering their parent classes
            kryo.register(Class.forName("scala.collection.immutable.List$SerializationProxy"));
            kryo.register(Class.forName("scala.collection.immutable.Vector$SerializationProxy"));
            kryo.register(Class.forName("scala.collection.immutable.Set$SerializationProxy"));
            kryo.register(Class.forName("scala.collection.immutable.Map$SerializationProxy"));
            
            // Now register the actual collections
            kryo.register(Class.forName("scala.collection.immutable.List"));
            kryo.register(Class.forName("scala.collection.immutable.Vector"));
            kryo.register(Class.forName("scala.collection.immutable.Set"));
            kryo.register(Class.forName("scala.collection.immutable.Map"));
            
            // Register Seq interface and its implementations
            kryo.register(Class.forName("scala.collection.Seq"));
            kryo.register(Class.forName("scala.collection.immutable.Seq"));
            kryo.register(Class.forName("scala.collection.mutable.Seq"));
            
            // Additional Scala collections needed for Spark
            kryo.register(Class.forName("scala.collection.immutable.Nil$"));
            kryo.register(Class.forName("scala.collection.immutable.$colon$colon"));
            kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"));
            kryo.register(Class.forName("scala.collection.immutable.Map$Map1"));
            kryo.register(Class.forName("scala.collection.immutable.Map$Map2"));
            kryo.register(Class.forName("scala.collection.immutable.Map$Map3"));
            kryo.register(Class.forName("scala.collection.immutable.Map$Map4"));
            kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"));
            kryo.register(Class.forName("scala.collection.immutable.Set$Set1"));
            kryo.register(Class.forName("scala.collection.immutable.Set$Set2"));
            
            // Mutable collections
            kryo.register(Class.forName("scala.collection.mutable.ArrayBuffer"));
            kryo.register(Class.forName("scala.collection.mutable.HashMap"));
            kryo.register(Class.forName("scala.collection.mutable.ListBuffer"));
            
            // Seq and collection interfaces
            kryo.register(Class.forName("scala.collection.generic.GenericCompanion"));
            
            // Registration of classes to solve serialization issues
            kryo.register(Class.forName("scala.collection.immutable.List$$anon$1"));
            kryo.register(Class.forName("scala.collection.AbstractSeq"));
            kryo.register(Class.forName("scala.collection.AbstractIterable"));
            kryo.register(Class.forName("scala.collection.AbstractTraversable"));
            
            // Register DataSourceRDDPartition class with a special serializer
            Class<?> dataSourceRDDPartitionClass = Class.forName("org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition");
            kryo.register(dataSourceRDDPartitionClass, new DataSourceRDDPartitionSerializer());
            
            // Registration of specific types for DataSourceRDDPartition
            kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition"));
            
            // Important: register the specific type used in the inputPartitions field
            kryo.register(Class.forName("scala.collection.immutable.List"));
            
            // Register classes used in Spark SQL for partition serialization
            kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.v2.DataSourceRDD"));
            kryo.register(Class.forName("org.apache.spark.sql.connector.read.InputPartition"));
            kryo.register(Class.forName("org.apache.spark.sql.connector.read.PartitionReader"));
            kryo.register(Class.forName("org.apache.spark.sql.connector.read.PartitionReaderFactory"));
            
        } catch (ClassNotFoundException e) {
            // Log the error but continue working
            System.err.println("Warning: Could not register some Scala collections: " + e.getMessage());
        }
        
        // Register Spark SQL classes
        try {
            // Low-level Spark SQL classes
            kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition"));
            kryo.register(Class.forName("org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema"));
            kryo.register(Class.forName("org.apache.spark.sql.catalyst.InternalRow"));
            kryo.register(Class.forName("org.apache.spark.sql.catalyst.expressions.UnsafeRow"));
            kryo.register(Class.forName("org.apache.spark.sql.execution.streaming.StreamExecution"));
            
            // Schema-related classes
            kryo.register(Class.forName("org.apache.spark.sql.types.StructType"));
            kryo.register(Class.forName("org.apache.spark.sql.types.StructField"));
            kryo.register(Class.forName("org.apache.spark.sql.types.Metadata"));
            kryo.register(Class.forName("org.apache.spark.sql.types.StringType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.IntegerType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.DoubleType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.TimestampType$"));
            
            // Spark's Kafka connector classes
            kryo.register(Class.forName("org.apache.spark.sql.kafka010.KafkaSourceRDD"));
            kryo.register(Class.forName("org.apache.spark.sql.kafka010.KafkaSourceRDDPartition"));
            
            // Window and other streaming-specific classes
            kryo.register(Class.forName("org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition"));
            kryo.register(Class.forName("org.apache.spark.sql.execution.streaming.WindowOutputMode"));
            kryo.register(Class.forName("org.apache.spark.sql.execution.streaming.StateStore$"));
            kryo.register(Class.forName("org.apache.spark.sql.execution.streaming.GroupStateImpl"));
            kryo.register(Class.forName("org.apache.spark.sql.execution.streaming.state.StateStoreProviderId"));
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: Could not register some Spark SQL classes: " + e.getMessage());
        }
        
        // Register specialized serializers
        kryo.register(ByteBuffer.class, new ByteBufferSerializer());
        kryo.register(BigDecimal.class, new BigDecimalSerializer());
        kryo.register(LocalDateTime.class, new LocalDateTimeSerializer());
        
        // Register specific implementations of ByteBuffer
        try {
            Class<? extends ByteBuffer> heapByteBufferClass = ByteBuffer.allocate(0).getClass();
            kryo.register(heapByteBufferClass, new ByteBufferSerializer());
            
            Class<? extends ByteBuffer> directByteBufferClass = ByteBuffer.allocateDirect(0).getClass();
            kryo.register(directByteBufferClass, new ByteBufferSerializer());
        } catch (Exception e) {
            System.err.println("Warning: Could not register ByteBuffer implementations: " + e.getMessage());
        }
    }
    
    /**
     * Special serializer for DataSourceRDDPartition that correctly handles
     * the inputPartitions field of type scala.collection.Seq
     */
    private static class DataSourceRDDPartitionSerializer extends Serializer<Object> {
        @Override
        public void write(Kryo kryo, Output output, Object object) {
            try {
                // Use reflection to access fields
                java.lang.reflect.Field indexField = object.getClass().getDeclaredField("index");
                indexField.setAccessible(true);
                int index = (int) indexField.get(object);
                
                java.lang.reflect.Field inputPartitionsField = object.getClass().getDeclaredField("inputPartitions");
                inputPartitionsField.setAccessible(true);
                Object inputPartitions = inputPartitionsField.get(object);
                
                // Write index
                output.writeInt(index);
                
                // Write inputPartitions as object
                kryo.writeClassAndObject(output, inputPartitions);
                
            } catch (Exception e) {
                System.err.println("Error serializing DataSourceRDDPartition: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        @Override
        public Object read(Kryo kryo, Input input, Class type) {
            try {
                // Read index
                int index = input.readInt();
                
                // Read inputPartitions as object
                Object inputPartitions = kryo.readClassAndObject(input);
                
                // Create new instance of DataSourceRDDPartition through constructor
                Object instance = type.getDeclaredConstructor(int.class, Object.class).newInstance(index, inputPartitions);
                
                return instance;
            } catch (Exception e) {
                System.err.println("Error deserializing DataSourceRDDPartition: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
    }
    
    /**
     * Specialized serializer for ByteBuffer that correctly handles
     * both HeapByteBuffer and DirectByteBuffer.
     */
    private static class ByteBufferSerializer extends Serializer<ByteBuffer> {
        @Override
        public void write(Kryo kryo, Output output, ByteBuffer buffer) {
            // Save position and limit
            int position = buffer.position();
            int limit = buffer.limit();
            int capacity = buffer.capacity();
            
            // Write metadata
            output.writeInt(position);
            output.writeInt(limit);
            output.writeInt(capacity);
            output.writeBoolean(buffer.isDirect());
            
            // Write buffer contents
            byte[] array = new byte[limit - position];
            buffer.position(position);
            buffer.get(array);
            output.writeBytes(array);
            
            // Restore position
            buffer.position(position);
            buffer.limit(limit);
        }
        
        @Override
        public ByteBuffer read(Kryo kryo, Input input, Class<ByteBuffer> type) {
            // Read metadata
            int position = input.readInt();
            int limit = input.readInt();
            int capacity = input.readInt();
            boolean direct = input.readBoolean();
            
            // Read contents
            byte[] array = input.readBytes(limit - position);
            
            // Create new buffer
            ByteBuffer buffer = direct ? 
                ByteBuffer.allocateDirect(capacity) : 
                ByteBuffer.allocate(capacity);
            
            // Fill buffer and set position/limit
            buffer.position(0);
            buffer.put(array);
            buffer.position(position);
            buffer.limit(limit);
            
            return buffer;
        }
    }
    
    /**
     * Specialized serializer for BigDecimal to improve performance.
     */
    private static class BigDecimalSerializer extends Serializer<BigDecimal> {
        @Override
        public void write(Kryo kryo, Output output, BigDecimal bd) {
            if (bd == null) {
                output.writeBoolean(false);
                return;
            }
            output.writeBoolean(true);
            
            // Convert BigDecimal to string for reliable serialization
            String str = bd.toString();
            output.writeString(str);
        }
        
        @Override
        public BigDecimal read(Kryo kryo, Input input, Class<BigDecimal> type) {
            boolean notNull = input.readBoolean();
            if (!notNull) {
                return null;
            }
            
            // Read string and convert back to BigDecimal
            String str = input.readString();
            return new BigDecimal(str);
        }
    }
    
    /**
     * Specialized serializer for LocalDateTime to handle Java 8+ time classes.
     */
    private static class LocalDateTimeSerializer extends Serializer<LocalDateTime> {
        @Override
        public void write(Kryo kryo, Output output, LocalDateTime dateTime) {
            if (dateTime == null) {
                output.writeBoolean(false);
                return;
            }
            output.writeBoolean(true);
            
            // Store year, month, day, hour, minute, second, nano
            output.writeInt(dateTime.getYear());
            output.writeInt(dateTime.getMonthValue());
            output.writeInt(dateTime.getDayOfMonth());
            output.writeInt(dateTime.getHour());
            output.writeInt(dateTime.getMinute());
            output.writeInt(dateTime.getSecond());
            output.writeInt(dateTime.getNano());
        }
        
        @Override
        public LocalDateTime read(Kryo kryo, Input input, Class<LocalDateTime> type) {
            boolean notNull = input.readBoolean();
            if (!notNull) {
                return null;
            }
            
            // Read components and reconstruct LocalDateTime
            int year = input.readInt();
            int month = input.readInt();
            int day = input.readInt();
            int hour = input.readInt();
            int minute = input.readInt();
            int second = input.readInt();
            int nano = input.readInt();
            
            return LocalDateTime.of(year, month, day, hour, minute, second, nano);
        }
    }
}