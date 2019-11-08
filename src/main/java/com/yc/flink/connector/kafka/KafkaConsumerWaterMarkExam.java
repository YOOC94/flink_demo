package com.yc.flink.connector.kafka;

import com.yc.flink.model.StudentEvent;
import com.yc.flink.schema.StudentEventSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Properties;

public class KafkaConsumerWaterMarkExam {
    public static void main(String[] args) throws Exception{

        ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.has("topic") ? params.get("topic") : "yc_station";
        String groupId = params.has("groupId") ? params.get("groupId") : "yc_test";

        Properties prop = new Properties();
        prop.setProperty("zookeeper.connect","192.168.101.216:2181");
        prop.setProperty("group.id",groupId);
        prop.setProperty("bootstrap.servers","192.168.101.216:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer010<StudentEvent> consumer = new FlinkKafkaConsumer010<>(topic, new StudentEventSchema(), prop);

        //从指定offset 开始消费数据
        HashMap<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("yc_station", 0), 128857L);



//        consumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter())
//        consumer.setStartFromTimestamp(1573202359897L);
//        consumer.setStartFromEarliest();
        DataStream<StudentEvent> source = env.addSource(consumer);
        source.map(new MapFunction<StudentEvent, StudentEvent>() {
            @Override
            public StudentEvent map(StudentEvent studentEvent) throws Exception {
                System.out.println("kafka receive data:  ==  " + studentEvent.toString());
                return studentEvent;
            }
        });

        env.execute();

    }
}
