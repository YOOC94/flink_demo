package com.yc.flink.time_and_watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class MyTrigger extends Trigger<Tuple3<String, Integer, Long>, TimeWindow> {

//    TriggerResult.CONTINUE  什么都不做
//    TriggerResult.FIRE  触发计算
//    TriggerResult.FIRE_AND_PURGE  触发计算并随后清除窗口中的元素
//    TriggerResult.PURGE  清除窗口中的元素

    @Override
    // 为添加到窗口的每个元素调用方法
    public TriggerResult onElement(Tuple3<String, Integer, Long> in, long l, TimeWindow timeWindow, TriggerContext tCtx) throws Exception {
        System.out.println("maxTimestamp:: " + timeWindow.maxTimestamp());
        System.out.println("currentWatermark :: " + tCtx.getCurrentWatermark());
        if (timeWindow.maxTimestamp() <= tCtx.getCurrentWatermark()){
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    // 当注册的事件时间计时器触发时，调用此方法
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext tCtx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    //当注册的处理时间计时器触发时，将调用此方法。
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext tCtx) throws Exception {
        System.out.println("currentWatermark: "  + tCtx.getCurrentWatermark());
        return  TriggerResult.FIRE;
    }

    @Override
    //方法在删除相应窗口后执行所需的任何操作
    public void clear(TimeWindow timeWindow, TriggerContext tCtx) throws Exception {
    }

    @Override
    public boolean canMerge() {
        return true;
    }

}
