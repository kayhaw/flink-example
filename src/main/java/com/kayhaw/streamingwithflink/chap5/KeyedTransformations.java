package com.kayhaw.streamingwithflink.chap5;

import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 演示按主键的转化方法：keyBy, reduce
 * */
public class KeyedTransformations {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 设置水印间隔
        env.getConfig().setAutoWatermarkInterval(1000L);

        // 3. 读取传感器数据流并设置水印
        DataStream<SensorReading> sensorData =
                env.addSource(new SensorSource()).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<SensorReading>)
                                                (element, recordTimestamp) -> element.timestamp));
        // 1. 根据传感器id分组
        KeyedStream<SensorReading, String> keyed = sensorData.keyBy(r -> r.id);

        // 2. 计算每个传感器数据最高温度的reduce方法
        DataStream<SensorReading> maxTempPerSensor = keyed.reduce((r1, r2) -> {
            if (r1.temperature > r2.temperature) {
                return r1;
            } else {
                return r2;
            }
        });

        maxTempPerSensor.print();

        env.execute("Keyed Transformations Example");
    }
}
