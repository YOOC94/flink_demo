package com.yc.flink.schema;

import com.google.gson.Gson;
import com.yc.flink.model.StudentEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 定义kafka序列化、反序列化
 */
public class StudentEventSchema implements DeserializationSchema<StudentEvent>, SerializationSchema<StudentEvent> {

    private static final Gson gson = new Gson();

    @Override
    public StudentEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes),StudentEvent.class);
    }
    /**
     * 一直运行
     * @param studentEvent
     * @return
     */
    @Override
    public boolean isEndOfStream(StudentEvent studentEvent) {
        return false;
    }

    @Override
    public byte[] serialize(StudentEvent studentEvent) {
        return gson.toJson(studentEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<StudentEvent> getProducedType() {
        return TypeInformation.of(StudentEvent.class);
    }
}
