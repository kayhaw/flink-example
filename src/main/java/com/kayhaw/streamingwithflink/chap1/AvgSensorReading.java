package com.kayhaw.streamingwithflink.chap1;

import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AvgSensorReading {
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 使用事件时间而不是处理时间
         * 自Flink 1.12起默认使用事件时间
         * 因此setStreamTimeCharacteristic方法已过时，将其注释掉
         */
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 设置水印间隔
        env.getConfig().setAutoWatermarkInterval(1000L);

        // 3. 读取传感器数据流
        /**
         * assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks)接口已过时
         * 使用{@link assignTimestampsAndWatermarks(WatermarkStrategy)}代替
         */
        /*DataStream<SensorReading> sensorData =
                env.addSource(new SensorSource()).assignTimestampsAndWatermarks(new SensorTimeAssigner());*/

        DataStream<SensorReading> sensorData =
            env.addSource(new SensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<SensorReading>)
                                        (element, recordTimestamp) -> element.timestamp));

        DataStream<SensorReading> avgTemp = sensorData
                // 将华氏温度转为摄氏温度
                .map( r -> new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
                // 按传感器id分流
                .keyBy(r -> r.id)
                // 对每个子流开启一个1s的滚动窗口
                // timeWindow(Time.seconds(1))已过时，它实际上是下面代码的封装
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                // 使用自定义方法计算平均温度
                .apply(new TemperatureAverager());

        avgTemp.print();

        // 执行应用
        env.execute("计算传感器平均温度");
    }

    /**
     * 用户自定义的窗口函数，计算传感器的平均温度值
     */
    public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        /**
         * apply()方法在每个窗口上被调用一次
         * @param sensorId 窗口主键(传感器id)
         * @param window 窗口元数据
         * @param input 接收传感器数值的迭代器
         * @param out 输出处理结果的收集器
         */
        @Override
        public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) {

            // 计算平均温度
            int cnt = 0;
            double sum = 0.0;
            for (SensorReading r : input) {
                cnt++;
                sum += r.temperature;
            }
            double avgTemp = sum / cnt;

            // 发送平均温度结果，window.getEnd()是最后一条记录的时间戳
            out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
        }
    }
}
