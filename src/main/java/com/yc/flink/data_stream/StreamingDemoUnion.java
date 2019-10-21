package com.yc.flink.data_stream;

import com.yc.flink.data_source.MyParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName:StreamingDemoUnion
 * @Auther: YC
 * @Date: 2019/10/21 23:19
 * @Description:  Connect：和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法。
 */
public class StreamingDemoUnion {
    public static void main(String[] args) throws Exception{

       //获取Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将source1 和 source2 组装到一起
        DataStream<Tuple2<String, Integer>> source1 = env.addSource( new MyParalleSource() ).setParallelism( 1 );
        DataStream<Tuple2<String,Integer>> source2 = env.addSource( new MyParalleSource() ).setParallelism( 1 );

        DataStream<Tuple2<String, Integer>> source = source1.union( source2 );

        source.map( new MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                System.out.println("原始接收到的数据：" + value );
                return value;
            }
        } )
                .timeWindowAll( Time.seconds(2) )
                .sum( 1)
                .print().setParallelism( 1 );
        String jobName = StreamingDemoUnion.class.getSimpleName();
        env.execute( jobName );
    }
}
