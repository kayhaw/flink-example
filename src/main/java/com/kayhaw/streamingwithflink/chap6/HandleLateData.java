package com.kayhaw.streamingwithflink.chap6;

import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

public class HandleLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData =
            env.addSource(new SensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(
                        (SerializableTimestampAssigner<SensorReading>)
                            (element, recordTimestamp) -> element.timestamp));

        // 方式1：使用sideOutputLateData收集
        SingleOutputStreamOperator<Integer> process = sensorData.keyBy(r -> r.id)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .sideOutputLateData(new OutputTag<>("late-readings"))
            .process(new CountFunction());

        // 方式2：在ProcessWindowFunction中收集
        // SingleOutputStreamOperator<SensorReading> process = sensorData.process(new LateReadingFilter());

        DataStream<SensorReading> sideOutput = process.getSideOutput(new OutputTag<>("late-readings"));

        // 计算每隔10s窗口的记录个数，延迟时间5s
        SingleOutputStreamOperator<Tuple4<String, Long, Integer, String>> countPer10Secs = sensorData.keyBy(r -> r.id)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .allowedLateness(Time.seconds(5))
            .process(new UpdatingWindowCountFunction());

        sideOutput.print();

        env.execute("Handle Late Data");
    }

    public static class CountFunction extends ProcessWindowFunction<SensorReading, Integer, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<SensorReading> elements,
                            Collector<Integer> out) throws Exception {
            Iterator<SensorReading> iter = elements.iterator();
            int cnt = 0;
            while(iter.hasNext()){
                ++cnt;
                iter.next();
            }
            out.collect(cnt);
        }
    }

    public static class LateReadingFilter extends ProcessFunction<SensorReading, SensorReading> {
        public OutputTag<SensorReading> lateReadingOut = new OutputTag<>("late-readings");

        @Override
        public void processElement(SensorReading r, Context ctx, Collector<SensorReading> out) throws Exception {
            if(r.timestamp < ctx.timerService().currentWatermark()) {
                // 延迟数据
                ctx.output(lateReadingOut, r);
            } else {
                out.collect(r);
            }
        }
    }

    public static class UpdatingWindowCountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long,
        Integer, String>, String, TimeWindow> {
        @Override
        public void process(String id, Context context, Iterable<SensorReading> elements, Collector<Tuple4<String,
            Long, Integer, String>> out) throws Exception {
            int cnt = 0;
            Iterator<SensorReading> iter = elements.iterator();
            while(iter.hasNext()){
                ++cnt;
                iter.next();
            }
            ValueState<Boolean> isUpdate = context.windowState().getState(new ValueStateDescriptor<>("isUpdate"
                , Boolean.class));
            if(!isUpdate.value()) {
                out.collect(new Tuple4<>(id, context.window().getEnd(), cnt, "first"));
            } else {
                out.collect(new Tuple4<>(id, context.window().getEnd(), cnt, "update"));
            }
        }
    }

}
