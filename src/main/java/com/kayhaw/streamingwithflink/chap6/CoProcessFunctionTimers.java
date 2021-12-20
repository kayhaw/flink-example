package com.kayhaw.streamingwithflink.chap6;

import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CoProcessFunctionTimers {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<Tuple2<String, Long>> filterSwitches = env
            .fromElements(
                Tuple2.of("sensor_2", 10_000L),
                Tuple2.of("sensor_7", 60_000L));

        DataStream<SensorReading> readings = env.addSource(new SensorSource());

        DataStream<SensorReading> forwardedReadings = readings
            .connect(filterSwitches)
            .keyBy(r -> r.id, s -> s.f0)
            .process(new ReadingFilter());

        forwardedReadings.print();

        env.execute("Filter sensor readings");
    }

    public static class FreezingMonitor extends ProcessFunction<SensorReading, SensorReading> {
        OutputTag<String> freezingAlarmOutput = new OutputTag<>("freezing-alarms");

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if(value.temperature < 32.0) {
                ctx.output(freezingAlarmOutput, "Freezing Alarm for " + value.id);
            }
            out.collect(value);
        }
    }

    public static class ReadingFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {
        private ValueState<Boolean> forwardingEnabled;
        private ValueState<Long> disableTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            forwardingEnabled = getRuntimeContext().getState(
                new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN()));
            disableTimer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer", Types.LONG()));
        }

        @Override
        public void processElement1(SensorReading r, Context ctx, Collector<SensorReading> out) throws Exception {
            // 检查是否需要forward传感器记录
            Boolean forward = forwardingEnabled.value();
            if(forward != null && forward)  {
                out.collect(r);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> s, Context ctx, Collector<SensorReading> out) throws Exception {
            // 开启forward
            forwardingEnabled.update(Boolean.TRUE);
            // 设置定时器
            long timerTimestamp = ctx.timerService().currentProcessingTime() + s.f1;
            Long curTimerTimestamp = disableTimer.value();
            if(curTimerTimestamp == null || timerTimestamp > curTimerTimestamp) {
                // 删除当前定时器
                if(curTimerTimestamp != null) {
                    ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                }
                // 注册新定时器
                ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
                disableTimer.update(timerTimestamp);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            // 删除所有状态
            forwardingEnabled.clear();
            disableTimer.clear();
        }
    }
}
