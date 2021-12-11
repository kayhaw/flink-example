package com.kayhaw.streamingwithflink.chap5;

import com.kayhaw.streamingwithflink.chap5.util.Alert;
import com.kayhaw.streamingwithflink.chap5.util.SmokeLevel;
import com.kayhaw.streamingwithflink.chap5.util.SmokeLevelSource;
import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 模拟传感器检测到火灾风险发出警告
 * 应用接收传一个温度流和一个烟雾等级流
 * 当烟雾等级为HIGH并且温度超过阈值时，发出警报
 */
public class MultiStreamTransformations {

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

        // 4. 读取烟雾等级流
        DataStream<SmokeLevel> smokeData =
                env.addSource(new SmokeLevelSource()).setParallelism(1);

        // 5. 将传感器流按id分组
        KeyedStream<SensorReading, String> keyed = sensorData.keyBy(r -> r.id);

        // 6. 将两个流连接在一起，当温度和烟雾等级过高时触发警报
        DataStream<Alert> alerts = keyed
                .connect(smokeData.broadcast())
                .flatMap(new RaiseAlertFlatMap());

        alerts.print();

        env.execute("Multi-Stream Transformations Example");
    }

    public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {
        private SmokeLevel smokeLevel = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading tempReading, Collector<Alert> out) throws Exception {
            if(this.smokeLevel == SmokeLevel.HIGH && tempReading.temperature > 100) {
                out.collect(new Alert("Risk of fire!" + tempReading, tempReading.timestamp));
            }
        }

        @Override
        public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> out) throws Exception {
            this.smokeLevel = smokeLevel;
        }
    }
}
