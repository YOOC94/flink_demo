package com.yc.flink.data_source;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Timestamp;
import java.util.Random;

public class FlinkStudentDataSource extends RichParallelSourceFunction<Tuple4<String,Integer,String, Long>> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        Random random = new Random();

        while (isRunning){
            //姓名
            StringBuilder name = new StringBuilder();
            name.append(("A" + (char)('a' + random.nextInt(20))));
            //age 10-20
            int max=20;
            int min=10;
            int age = random.nextInt(max)%(max-min+1) + min;
            //sex
            String sex = age % 2 == 1 ? "man" : "woman";
            //timestap
            long timestap = System.currentTimeMillis();
            ctx.collectWithTimestamp(new Tuple4<>(name.toString(),age,sex,timestap),timestap);
            Thread.sleep(500L);
        }

    }

    @Override
    public void cancel() {

        isRunning = false;

    }
}
