package com.yc.flink.data_source;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName:DataSource
 * @Auther: YC
 * @Date: 2019/10/20 21:24
 * @Description: flink 程序的数据源  flink自定义了许多source
 */
public class MyDataSource implements SourceFunction<Long> {

    private long count = 1L;
    private boolean isRunning = true;

    /**
     * 启动一个source
     * 在run方法中实现一个循环，这样就可以循环产生数据
     */

    public void run(SourceContext<Long> ctx) throws Exception {

        while (isRunning){
            ctx.collect( count );
            count++;
            //每秒产生一条数据
            Thread.sleep( 1000 );
        }
    }
    public void cancel() {
        isRunning = false;
    }
}

