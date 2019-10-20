package com.yc.flink.data_stream;

import com.yc.flink.data_source.MyDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingDemoWitMyDataSource {
    public static void main(String[] args) throws Exception{

        //获取Flink运行换进
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> source = env.addSource( new MyDataSource() ).setParallelism( 1 );//并行度设置为1
        DataStream<Long> map = source.map( new MapFunction<Long, Long>() {
            public Long map(Long aLong) throws Exception {
                System.out.println( "接收到数据：" + aLong );
                return aLong;
            }
        });

        DataStream<Long> sum = map.timeWindowAll( Time.seconds( 2 ) ).sum( 0 );
        sum.print().setParallelism( 1 );
        String jobName = StreamingDemoWitMyDataSource.class.getSimpleName();
        env.execute(jobName);
    }

}
