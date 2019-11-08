package com.yc.flink.connector.kafka;

import com.yc.flink.data_source.FlinkStudentDataSource;
import com.yc.flink.model.StudentEvent;
import com.yc.flink.schema.StudentEventSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

public class KafkaProducerExample {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple4<String, Integer, String, Long>> studentSource = env.addSource(new FlinkStudentDataSource());


        DataStream<Tuple4<String, Integer, String, Long>> mapStream =
                studentSource.map(new MapFunction<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>>() {
            @Override
            public Tuple4<String, Integer, String, Long> map(Tuple4<String, Integer, String, Long> input) throws Exception {
                System.out.println("接收到的数据" + input);
                return input;
            }
        });

        //配置kafka sink

        FlinkKafkaProducer010<StudentEvent> kafkaSink =
                new FlinkKafkaProducer010<>("192.168.101.216:9092", "yc_station", new StudentEventSchema());
        kafkaSink.setWriteTimestampToKafka(true);

        mapStream.map(new MapFunction<Tuple4<String, Integer, String, Long>, StudentEvent>() {
            @Override
            public StudentEvent map(Tuple4<String, Integer, String, Long> in) throws Exception {
                return new StudentEvent(in.f0,in.f3,in.f2,in.f1);
            }
        }).addSink(kafkaSink);

        env.execute();
    }
}
