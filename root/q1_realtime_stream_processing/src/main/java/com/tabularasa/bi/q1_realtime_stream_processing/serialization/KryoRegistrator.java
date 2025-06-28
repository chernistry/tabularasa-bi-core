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
        
        // Регистрируем специализированный сериализатор для ByteBuffer
        kryo.register(ByteBuffer.class, new ByteBufferSerializer());
        
        // Регистрируем конкретные реализации ByteBuffer
        Class<? extends ByteBuffer> heapByteBufferClass = ByteBuffer.allocate(0).getClass();
        kryo.register(heapByteBufferClass, new ByteBufferSerializer());
        
        Class<? extends ByteBuffer> directByteBufferClass = ByteBuffer.allocateDirect(0).getClass();
        kryo.register(directByteBufferClass, new ByteBufferSerializer());
    }
    
    /**
     * Специализированный сериализатор для ByteBuffer, который корректно обрабатывает
     * как HeapByteBuffer, так и DirectByteBuffer.
     */
    private static class ByteBufferSerializer extends Serializer<ByteBuffer> {
        @Override
        public void write(Kryo kryo, Output output, ByteBuffer buffer) {
            // Сохраняем позицию и лимит
            int position = buffer.position();
            int limit = buffer.limit();
            int capacity = buffer.capacity();
            
            // Записываем метаданные
            output.writeInt(position);
            output.writeInt(limit);
            output.writeInt(capacity);
            output.writeBoolean(buffer.isDirect());
            
            // Записываем содержимое буфера
            byte[] array = new byte[limit - position];
            buffer.position(position);
            buffer.get(array);
            output.writeBytes(array);
            
            // Восстанавливаем позицию
            buffer.position(position);
            buffer.limit(limit);
        }
        
        @Override
        public ByteBuffer read(Kryo kryo, Input input, Class<ByteBuffer> type) {
            // Читаем метаданные
            int position = input.readInt();
            int limit = input.readInt();
            int capacity = input.readInt();
            boolean direct = input.readBoolean();
            
            // Читаем содержимое
            byte[] array = input.readBytes(limit - position);
            
            // Создаем новый буфер
            ByteBuffer buffer = direct ? 
                ByteBuffer.allocateDirect(capacity) : 
                ByteBuffer.allocate(capacity);
            
            // Заполняем буфер и устанавливаем позицию/лимит
            buffer.position(0);
            buffer.put(array);
            buffer.position(position);
            buffer.limit(limit);
            
            return buffer;
        }
    }
} 