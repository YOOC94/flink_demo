package com.yc.flink.data_stream;

import com.yc.flink.data_source.MyParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName:StreamingDemoWithPralalleSource
 * @Auther: YC
 * @Date: 2019/10/20 22:52
 * @Description:
 */
public class StreamingDemoWithPralalleSource {
    public static void main(String[] args) throws Exception {
        //获取Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource source = env.addSource( new MyParalleSource() ).setParallelism( 2 );
        DataStream map = source.map( new MapFunction<Long, Long>() {
            public Long map(Long aLong) throws Exception {
                System.out.println( "接收到数据：" + aLong );
                return aLong;
            }
        } );

        DataStream sum = map.timeWindowAll( Time.seconds( 2 ) ).sum( 0 );

        sum.print().setParallelism( 1 );
        String simpleName = StreamingDemoWithPralalleSource.class.getSimpleName();
        env.execute( simpleName );
    }
}
