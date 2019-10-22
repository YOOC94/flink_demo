package com.yc.flink.data_stream;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        DataStreamSource<Integer> source1 = env.fromCollection(source1Data);
        DataStreamSource<Integer> source2 = env.fromCollection(source2Data);

//                DataStream<Integer> source = source1.union(source2);
//        source.map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer value) throws Exception {
//
//                System.out.println("接受到的数据: " + value);
//                return value;
//            }
//        }).print().setParallelism(1);

        ConnectedStreams<Integer, Integer> source = source1.connect(source2);

        source.getFirstInput().map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 10;
            }
        }).print();

        source.getSecondInput().map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        }).print();

        String jobName = StreamingDemoUnion.class.getSimpleName();
        env.execute( jobName );
    }
}
