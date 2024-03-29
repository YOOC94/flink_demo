package com.yc.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

    private transient ValueState<Tuple2<Long,Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> output) throws Exception {

        Tuple2<Long, Long> currentSum = sum.value();
        //更新count
        currentSum.f0 += 1;
        //更新vlaue
        currentSum.f1 += input.f1;
        sum.update(currentSum);
        if (currentSum.f0 >= 2){
            output.collect(new Tuple2<Long,Long>(input.f0,currentSum.f1 / currentSum.f0));
//            sum.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average" //状态名称
                , TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}) //类型信息
                , Tuple2.of(0L, 0L) //状态默认值
        );

        sum = getRuntimeContext().getState(descriptor);
    }
}
