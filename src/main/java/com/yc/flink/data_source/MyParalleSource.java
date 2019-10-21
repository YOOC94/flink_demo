package com.yc.flink.data_source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.util.Random;

/**
 * @ClassName:MyParalleSource
 * @Auther: YC
 * @Date: 2019/10/20 22:43
 * @Description: 继承 RichParallelSourceFunction 的子类 ParallelSourceFunction
 * 可以并行生产数据
 * https://blog.csdn.net/shenshouniu/article/details/84728884
 */
public class MyParalleSource extends RichParallelSourceFunction<Tuple2<String,Integer>> {

    private boolean isRunning = true;

    /**
     * 循环run方法生产数据
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        while (isRunning){

            String key  = "类别" + (char)('A' + random.nextInt(3));
            int value = random.nextInt(10) + 1;
            System.out.println(String.format("发送数据:\t (%s,%d)",key,value));
            ctx.collect( new Tuple2<>(key,value));
            //每秒生产数据
            Thread.sleep( 1000 );
        }
    }
    public void cancel() {
        isRunning = false;
    }
}
