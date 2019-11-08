package com.yc.flink.connector.kafka;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;


import java.util.Properties;

/**
 * flink connect kafka demo
 */
public class KafkaConsumerExample {
    public static void main(String[] args) throws Exception{

        ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.has("topic") ? params.get("topic") : "yc_station";
        String groupId = params.has("groupId") ? params.get("groupId") : "yc_test";

        Properties prop = new Properties();
        prop.setProperty("zookeeper.connect","192.168.101.216:2181");
        prop.setProperty("group.id",groupId);
        prop.setProperty("bootstrap.servers","192.168.101.216:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.enableCheckpointing(5000L);
        //topicList
//        FlinkKafkaConsumer010<String> consumerList = new FlinkKafkaConsumer010<>(new ArrayList<>(), new SimpleStringSchema(), prop);
//        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(Pattern.compile("topic[0-9]"), new SimpleStringSchema(), prop);
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), prop);

//        HashMap<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
//        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
//        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L);
//        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L);
//        consumer.setStartFromSpecificOffsets(specificStartOffsets);



        //        myConsumer.setStartFromEarliest();     // start from the earliest record possible
        //        myConsumer.setStartFromLatest();       // start from the latest record
        //        myConsumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
        //        myConsumer.setStartFromGroupOffsets(); // the default behaviour
        DataStreamSource<String> kafkaSource = env.addSource(consumer);
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> windowStream = kafkaSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).keyBy(0).countWindow(5);


        windowStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<>(t2.f0,t1.f1+t2.f1);
            }
        }).print();
        env.execute();
    }
}
