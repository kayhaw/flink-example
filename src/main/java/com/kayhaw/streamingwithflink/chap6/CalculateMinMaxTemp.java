package com.kayhaw.streamingwithflink.chap6;

import com.kayhaw.streamingwithflink.util.MinMaxTemp;
import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class CalculateMinMaxTemp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData =
            env.addSource(new SensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(
                        (SerializableTimestampAssigner<SensorReading>)
                            (element, recordTimestamp) -> element.timestamp));

        DataStream<MinMaxTemp> minMax = sensorData.map(r -> new Tuple3<>(r.id, r.temperature,
            r.temperature))
            .keyBy(r -> r.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                (r1, r2) -> new Tuple3<>(r1.f0, Math.min(r1.f1, r2.f1), Math.max(r1.f2, r2.f2)),
                new AssignWindowEndProcessFunction()
            );

        minMax.print();

        env.execute("Calculate min and max temperature");
    }

    public static class AssignWindowEndProcessFunction extends
        ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow> {

        @Override
        public void process(String key, Context ctx, Iterable<Tuple3<String, Double, Double>> minMaxIt,
                            Collector<MinMaxTemp> out) throws Exception {
            Tuple3<String, Double, Double> minMax = minMaxIt.iterator().next();
            long end = ctx.window().getEnd();
            out.collect(new MinMaxTemp(key, minMax.f1, minMax.f2, end));
        }
    }

}
