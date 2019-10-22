package com.yc.flink.data_stream;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName:StreamingDemoUnion
 * @Auther: YC
 * @Date: 2019/10/21 23:19
 * @Description:  Connect：和union似类，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法。
 */
public class StreamingDemoUnion {
    public static void main(String[] args) throws Exception{

       //获取Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将source1 和 source2 组装到一起

        List<Integer> source1Data = new ArrayList<>();
        source1Data.add(1);
        source1Data.add(2);
        List<Integer> source2Data = new ArrayList<>();
        source2Data.add(11);
        source2Data.add(22);
        source1Data.add(3);
        source2Data.add(4);
        source1Data.add(5);
        source2Data.add(6);
        source1Data.add(7);
        source2Data.add(8);
        DataStreamSource<Integer> source1 = env.fromCollection(source1Data);
        DataStreamSource<Integer> source2 = env.fromCollection(source2Data);

        ConnectedStreams<Integer, Integer> source = source1.connect(source2);

        source.map(new CoMapFunction<Integer, Integer, Integer>() {
            @Override
            public Integer map1(Integer value1) throws Exception {
                return value1 * 10;
            }

            @Override
            public Integer map2(Integer value2) throws Exception {
                return value2 * 2;
            }
        }).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value3) throws Exception {
                return value3 + 1;
            }
        }).countWindowAll(5,4).process(new ProcessAllWindowFunction<Integer, Integer, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                System.out.println(" 开始执行process" + System.currentTimeMillis());
                int i = 0;
                for (Integer element : elements) {
                    System.out.println(element);
                    i += element;
                }
                out.collect(i);
            }
        }).print();



//        source.getFirstInput().map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value * 10;
//            }
//        });
//
//        SingleOutputStreamOperator<Integer> map = source.getSecondInput().map(new MapFunction<Integer, Integer>() {
//
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value * 2;
//            }
//        });


        String jobName = StreamingDemoUnion.class.getSimpleName();
        env.execute( jobName );
    }
}
