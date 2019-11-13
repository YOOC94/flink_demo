package com.yc.flink.time_and_watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Random;

public class SessionWindowExam {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //自定义数据源
        DataStreamSource<Tuple3<String, Integer, Long>> source = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple3<String, Integer, Long>> context) throws Exception {
                Random random = new Random(System.currentTimeMillis());
                int i = 0;
                while (isRunning) {
                    String s = "A" + (char) ('a' + random.nextInt(10));
                    long timestamp = System.currentTimeMillis();
                    context.collectWithTimestamp(new Tuple3<>(s, 1, timestamp), timestamp);
                    context.emitWatermark(new Watermark(timestamp));
                    if (i%5 == 0 && i != 0){
                        Thread.sleep(5000);
                    }else {
                        Thread.sleep(1000L);
                    }
                    i++;
                }
            }
            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // 业务流程
        DataStream<Tuple3<String, Integer, Long>> map = source.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> mapIn) throws Exception {
                System.out.println("接收到的数据:::  " + mapIn);
                return mapIn;
            }
        });

        WindowedStream<Tuple3<String, Integer, Long>, Tuple, TimeWindow> window =
                map.keyBy(0).window(EventTimeSessionWindows.withGap(Time.milliseconds(5000L)));
        window.sum(1).print();

        env.execute();

    }

    //sessionWindow 在面对并发量大的时候如何做实时推荐？
}
