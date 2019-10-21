package com.yc.flink.data_stream;

import com.yc.flink.data_source.MyParalleSource;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.util.HashMap;
import java.util.Map;

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

        DataStream<Tuple2<String, Integer>> dSource = env.addSource(new MyParalleSource()).setParallelism(2);

        KeyedStream<Tuple2<String, Integer>, Tuple> ks = dSource.keyBy(0);
        ks.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> fold(Map<String, Integer> accomutor, Tuple2<String, Integer> o) throws Exception {
                accomutor.put(o.f0,o.f1);
                return accomutor;
            }
        }).addSink(new SinkFunction<Map<String, Integer>>() {
            @Override
            public void invoke(Map<String, Integer> value, Context context) throws Exception {
                System.out.println(value.values().stream().mapToInt(v -> v).sum());
            }
        });

        //提交任务
        String simpleName = StreamingDemoWithPralalleSource.class.getSimpleName();
        env.execute( simpleName );
    }
}
