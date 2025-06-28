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
        
        // Register Scala collections
        try {
            kryo.register(Class.forName("scala.collection.immutable.List"));
            kryo.register(Class.forName("scala.collection.immutable.List$SerializationProxy"));
            kryo.register(Class.forName("scala.collection.immutable.Vector"));
            kryo.register(Class.forName("scala.collection.immutable.Vector$"));
            kryo.register(Class.forName("scala.collection.immutable.$colon$colon"));
            kryo.register(Class.forName("scala.collection.immutable.Nil$"));
            kryo.register(Class.forName("scala.collection.mutable.ArrayBuffer"));
            kryo.register(Class.forName("scala.collection.mutable.HashMap"));
            kryo.register(Class.forName("scala.collection.mutable.ListBuffer"));
            kryo.register(Class.forName("scala.collection.Seq"));
            kryo.register(Class.forName("scala.collection.immutable.Seq"));
            kryo.register(Class.forName("scala.collection.generic.GenericCompanion"));
            kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"));
        } catch (ClassNotFoundException e) {
            // Log the error but continue working
            System.err.println("Warning: Could not register some Scala collections: " + e.getMessage());
        }
        
        // Register Spark SQL classes
        try {
            kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition"));
            kryo.register(Class.forName("org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema"));
            kryo.register(Class.forName("org.apache.spark.sql.types.StructType"));
            kryo.register(Class.forName("org.apache.spark.sql.types.StructField"));
            kryo.register(Class.forName("org.apache.spark.sql.types.Metadata"));
            kryo.register(Class.forName("org.apache.spark.sql.types.StringType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.IntegerType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.DoubleType$"));
            kryo.register(Class.forName("org.apache.spark.sql.types.TimestampType$"));
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: Could not register some Spark SQL classes: " + e.getMessage());
        }
        
        // Register a specialized serializer for ByteBuffer
        kryo.register(ByteBuffer.class, new ByteBufferSerializer());
        
        // Register specific implementations of ByteBuffer
        Class<? extends ByteBuffer> heapByteBufferClass = ByteBuffer.allocate(0).getClass();
        kryo.register(heapByteBufferClass, new ByteBufferSerializer());
        
        Class<? extends ByteBuffer> directByteBufferClass = ByteBuffer.allocateDirect(0).getClass();
        kryo.register(directByteBufferClass, new ByteBufferSerializer());
        
        // Register BigDecimal serializer for better performance
        kryo.register(BigDecimal.class, new BigDecimalSerializer());
        
        // Register LocalDateTime serializer
        kryo.register(LocalDateTime.class, new LocalDateTimeSerializer());
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