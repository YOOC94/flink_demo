package com.yc.flink.data_source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName:MyParalleSource
 * @Auther: YC
 * @Date: 2019/10/20 22:43
 * @Description: 继承 RichParallelSourceFunction 的子类 ParallelSourceFunction
 * 可以并行生产数据
 * https://blog.csdn.net/shenshouniu/article/details/84728884
 */
public class MyParalleSource implements ParallelSourceFunction<Long>{

    private long count = 1L;
    private boolean isRunning = true;

    /**
     * 循环run方法生产数据
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext ctx) throws Exception {
        while (isRunning){
            ctx.collect( count );
            count++;
            //每秒生产数据
            Thread.sleep( 1000 );
        }
    }
    public void cancel() {
        isRunning = false;
    }
}
