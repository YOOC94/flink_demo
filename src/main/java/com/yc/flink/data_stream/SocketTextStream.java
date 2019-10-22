package com.yc.flink.data_stream;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: SocketTextStream
 * @Description: TODO
 * @Author: admin
 * @Date: 2019/10/21 15:09
 * @Version: 1.0
 **/
public class SocketTextStream {
    public static void main(String[] args) throws Exception{

        //获取需要的端口号

        int port;
        String hostname = "192.168.101.216";
        String delimeter = "\n";
        Map<String, Integer> map = new HashMap<>();
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default 9100");
            port = 8888;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dSource = env.socketTextStream(hostname, port, delimeter);

        dSource.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Map<String,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Map<String,Integer>> out) throws Exception {
                        System.out.println("执行process");
                        for (Tuple2<String, Integer> element : elements) {
                            if (!map.containsKey( element.f0 )){
                                map.put(element.f0,element.f1);
                            }else {
                                int i = map.get( element.f0 ) + 1;
                                map.put( element.f0,i );
                            }
                        }
                        out.collect(map);
                    }
                }).print();
        env.execute( "Socket window count" );

//        dSource.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
//            @Override
//            public Tuple2<Integer,Integer> map(String s) throws Exception {
//                return new Tuple2<Integer, Integer>(1,Integer.parseInt(s));
//            }
//        }).keyBy(0)
//                .timeWindow(Time.seconds(5))
//                //ProcessWindowFunction 参数介绍
//                //这里面的参数有三对。
//                //第一对是上游stream中的element类型，比如上游是Tuple[String,String]，这里就是[String，String]
//                //第三对是你要输出的结果的类型
//                //中间的是从第一对转化成第三对需要的临时类型
//                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Tuple, TimeWindow>() {
//                    @Override
//                    public void process(Tuple key, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
//                        System.out.println("执行process");
//                        long count = 0;
//                        for (Tuple2<Integer, Integer> element : elements) {
//                            count++;
//                        }
//                        System.out.println("key:=======>>\t"+key);
//                        out.collect("window:"+context.window()+",count:"+count);
//                    }
//                }).print();
//        env.execute("Socket window count");
    }
}

