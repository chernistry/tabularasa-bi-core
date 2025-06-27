package com.tabularasa.bi.q1_realtime_stream_processing.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import java.nio.ByteBuffer;

public class KryoRegistrator implements org.apache.spark.serializer.KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(AdEvent.class);
        // Register a safe serializer for java.nio.ByteBuffer to avoid reflection issues on JDK 17+
        kryo.addDefaultSerializer(ByteBuffer.class, new JavaSerializer());
        kryo.register(ByteBuffer.class);
        // Register specific ByteBuffer implementations to ensure JavaSerializer is used
        Class<? extends ByteBuffer> heapByteBufferClass = ByteBuffer.allocate(0).getClass();
        kryo.register(heapByteBufferClass, new JavaSerializer());
        Class<? extends ByteBuffer> directByteBufferClass = ByteBuffer.allocateDirect(0).getClass();
        kryo.register(directByteBufferClass, new JavaSerializer());
    }
} 