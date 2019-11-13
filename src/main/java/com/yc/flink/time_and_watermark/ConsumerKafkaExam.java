package com.yc.flink.time_and_watermark;

import com.yc.flink.model.StudentEvent;
import com.yc.flink.schema.StudentEventSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Properties;

public class ConsumerKafkaExam {
    public static void main(String[] args) throws Exception{

        ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.has("topic") ? params.get("topic") : "yc_station";
        String groupId = params.has("groupId") ? params.get("groupId") : "yc_test";

        Properties prop = new Properties();
        prop.setProperty("zookeeper.connect","192.168.101.216:2181");
        prop.setProperty("group.id",groupId);
        prop.setProperty("bootstrap.servers","192.168.101.216:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer010<StudentEvent> consumer = new FlinkKafkaConsumer010<>(topic, new StudentEventSchema(), prop);

        // 生成watermark 周期性生成
        FlinkKafkaConsumerBase<StudentEvent> assignConsumer = consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<StudentEvent>() {
            private final Long maxOutofLate = 5 * 1000L;
            private Long currentTimestamp = Long.MIN_VALUE;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                Watermark watermark = new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxOutofLate);
                System.out.println("Watermark Value is :  " + watermark.toString());
                return watermark;
            }

            @Override
            public long extractTimestamp(StudentEvent studentEvent, long l) {
                if (currentTimestamp <= studentEvent.getTimestamp()) {
                    currentTimestamp = studentEvent.getTimestamp();
                }
                return currentTimestamp;
            }
        });

        // 设置自动生成waternark 间隔
        env.getConfig().setAutoWatermarkInterval(5*1000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<StudentEvent> source = env.addSource(assignConsumer);

        source.map(new MapFunction<StudentEvent, StudentEvent>() {
            @Override
            public StudentEvent map(StudentEvent studentEvent) throws Exception {
                System.out.println(studentEvent.toString());
                return studentEvent;
            }
        }).keyBy(studentEvent ->  studentEvent.getSex())
                .timeWindow(Time.milliseconds(5*1000L))
                .apply(new WindowFunction<StudentEvent, Tuple2<String,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<StudentEvent> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int m = 0;
                        int w = 0;
                        for (StudentEvent studentEvent : input) {
                            if (studentEvent.getSex().equals("man")){
                                m++;
                            }else{
                                w++;
                            }
                        }
                        out.collect(new Tuple2<>("man",m));
                        out.collect(new Tuple2<>("woman",w));
                    }
                }).print();

        env.execute();

    }
}
