package com.yc.flink.time_and_watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SessionWindowExamForSocket {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000L);
        DataStreamSource<String> socketSource = env.socketTextStream("192.168.101.216", 9999, "\n");
        DataStream<Tuple3<String, Integer, Long>> map = socketSource.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String s) throws Exception {
                System.out.println("接收到的数据:: " + s);
                String[] split = s.split(",");
                return new Tuple3<>(split[0], Integer.parseInt(split[1]), System.currentTimeMillis());
            }
        });

        DataStream<Tuple3<String, Integer, Long>> assigner =
                map.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, Integer, Long> stringIntegerLongTuple3) {
                        return 0;
                    }
                });
        assigner
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(5*1000L)))
                .trigger(new MyTrigger())
                .sum(1)
                .print();

        env.execute();


    }
}
