package com.yc.flink.data_stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @ClassName:StreamingFromCollection
 * @Auther: YC
 * @Date: 2019/10/21 22:37
 * @Description:
 */
public class StreamingFromCollection {
    public static void main(String[] args) throws Exception{
        //获取Fklink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> data = new ArrayList<>();
        data.add( 10 );
        data.add( 15 );
        data.add( 20 );

        //指定数据源
        DataStreamSource<Integer> collectionData = env.fromCollection( data );
        //使用Map处理数据
        collectionData.map( new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value+1;
            }
        }).print().setParallelism( 1 );

        env.execute( "StreamingFromCollection" );

    }
}
