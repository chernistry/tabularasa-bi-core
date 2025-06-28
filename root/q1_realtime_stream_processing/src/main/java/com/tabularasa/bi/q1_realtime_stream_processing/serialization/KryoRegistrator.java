package com.tabularasa.bi.q1_realtime_stream_processing.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import java.nio.ByteBuffer;

public class KryoRegistrator implements org.apache.spark.serializer.KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(AdEvent.class);
        
        // Register a specialized serializer for ByteBuffer
        kryo.register(ByteBuffer.class, new ByteBufferSerializer());
        
        // Register specific implementations of ByteBuffer
        Class<? extends ByteBuffer> heapByteBufferClass = ByteBuffer.allocate(0).getClass();
        kryo.register(heapByteBufferClass, new ByteBufferSerializer());
        
        Class<? extends ByteBuffer> directByteBufferClass = ByteBuffer.allocateDirect(0).getClass();
        kryo.register(directByteBufferClass, new ByteBufferSerializer());
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
}