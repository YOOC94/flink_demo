package com.yc.flink.data_stream;

import com.yc.flink.data_source.MyParalleSource;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Objects;

/**
 * @ClassName:StreamingDemoFiltr
 * @Auther: YC
 * @Date: 2019/10/21 22:46
 * @Description:
 */
public class StreamingDemoFiltr {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> filterTest = env.addSource( new MyParalleSource() ).setParallelism( 1 );
        DataStream<Tuple2<String,Integer>> map = filterTest.map( new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> reveiverTuple) throws Exception {
                System.out.println("接收到的数据: " + reveiverTuple);
                return reveiverTuple;
            }
        } );
        //执行Filter过滤，满足条件的数据会被留下来
        DataStream<Tuple2<String,Integer>> filter = map.filter( new FilterFunction<Tuple2<String,Integer>>() {
            @Override
            public boolean filter(Tuple2 value) throws Exception {
                return  (Integer)value.f1 % 2 == 0;
            }
        });
        filter.map( new MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                System.out.println("过滤后的数据: " + value);
                return value;
            }
        }).print().setParallelism( 1 );

        String jobName = StreamingDemoFiltr.class.getSimpleName();
        env.execute( jobName );
    }
}
