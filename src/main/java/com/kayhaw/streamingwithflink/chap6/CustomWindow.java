package com.kayhaw.streamingwithflink.chap6;

import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

public class CustomWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        DataStream<SensorReading> sensorData =
            env.addSource(new SensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(
                        (SerializableTimestampAssigner<SensorReading>)
                            (element, recordTimestamp) -> element.timestamp));

        DataStream<Tuple4<String, Long, Long, Integer>> countsPerThirtySecs = sensorData.keyBy(r -> r.id)
            .window(new ThirtySecondsWindows())
            .trigger(new OneSecondIntervalTrigger())
            .process(new CountFunction());

        countsPerThirtySecs.print();

        env.execute("Custom Window Example");
    }

    /** 自定义Assigner */
    public static class ThirtySecondsWindows extends WindowAssigner<Object, TimeWindow> {
        long windowSize = 30_000L;

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long ts, WindowAssignerContext context) {
            // 30秒对齐
            long startTime = ts - (ts % windowSize);
            long endTime = startTime + windowSize;
            // 发送到对应窗口
            return Collections.singletonList(new TimeWindow(startTime, endTime));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return false;
        }

    }

    /** 自定义Trigger */
    public static class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow> {

        @Override
        public TriggerResult onElement(SensorReading r, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // firstSeen默认为false
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen",
                Types.BOOLEAN()));
            // 只有第一个元素会初始化
            if(!firstSeen.value()) {
                // 计算下一个
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                ctx.registerEventTimeTimer(t);
                ctx.registerEventTimeTimer(window.getEnd());
                firstSeen.update(Boolean.TRUE);
            }
            // 后续元素什么也不干
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long ts, TimeWindow window, TriggerContext ctx) throws Exception {
            if (ts == window.getEnd()) {
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                // 注册下一个定时器
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                if (t < window.getEnd()) {
                    ctx.registerEventTimeTimer(t);
                }
                return TriggerResult.FIRE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // 使用事件时间，处理时间不关心，跳过
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // 清除自定义状态信息
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen",
                Types.BOOLEAN()));
            firstSeen.clear();
        }

    }

    /** 自定义ProcessWindowFunction */
    public static class CountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long,
        Integer>, String, TimeWindow> {
        @Override
        public void process(String id, Context context, Iterable<SensorReading> elements, Collector<Tuple4<String,
            Long, Long, Integer>> out) throws Exception {
            // 统计个数
            int cnt = 0;
            for (SensorReading e : elements) {
                ++cnt;
            }
            long evalTime = context.currentWatermark();
            out.collect(Tuple4.of(id, context.window().getEnd(), evalTime, cnt));
        }
    }
}
