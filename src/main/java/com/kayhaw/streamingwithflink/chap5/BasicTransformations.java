package com.kayhaw.streamingwithflink.chap5;

import com.kayhaw.streamingwithflink.util.SensorReading;
import com.kayhaw.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 演示3种基本的转化方法：filter, map和flatMap
 * */
public class BasicTransformations {

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

        // a) filter方法过滤掉温度小于25的传感器数据
        DataStream<SensorReading> filteredData = sensorData.filter(r -> r.temperature >= 25);

        // a') 使用具体的FilterFunction实现类代替lambda
        // DataStream<SensorReading> filteredData = sensorData.filter(new TemperatureFilter(25));

        // b) map方法将SensorReading流映射为传感器id流
        DataStream<String> sensorIds = sensorData.map(r -> r.id);

        // b') 使用具体的MapFunction实现类代替lambda
        // DataStream<String> sensorIds = sensorData.map(new IdExtractor());

        // c) flatMap方法将传感器id分为前缀"sensor"和id编号
        DataStream<String> splitIds = sensorIds
                .flatMap((FlatMapFunction<String, String>)
                        (id, out) -> { for (String s : id.split("_")) { out.collect(s); } })
                // Java不能推断lambda返回类型
                .returns(Types.STRING);

        // c') 使用具体的FlatMapFunction实现类代替lambda
        // DataStream<String> splitIds = sensorIds.flatMap(new IdSplitter());

        // 打印到stdout
        splitIds.print();

        // 运行应用
        env.execute("Basic Transformations Example");
    }

    /**
     * 自定义FilterFunction实现类
     */
    public static class TemperatureFilter implements FilterFunction<SensorReading> {
        private final double threshold;

        public TemperatureFilter(double threshold) { this.threshold = threshold; }

        @Override
        public boolean filter(SensorReading value) throws Exception {
            return value.temperature >= threshold;
        }
    }

    /**
     * 自定义MapFunction实现类
     */
    public static class IdExtractor implements MapFunction<SensorReading, String> {
        @Override
        public String map(SensorReading value) throws Exception {
            return value.id;
        }
    }

    /**
     * 自定义FlatMapFunction实现类
     */
    public static class IdSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String id, Collector<String> out) throws Exception {
            String[] splits = id.split("_");
            for (String split : splits) {
                out.collect(split);
            }
        }
    }
}
