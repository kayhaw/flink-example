package com.kayhaw.streamingwithflink.chap5;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 滚动求和示例
 */
public class RollingSum {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer, Integer, Integer>> inputStream = env.fromElements(
                Tuple3.of(1, 2, 2), Tuple3.of(2, 3, 1), Tuple3.of(2, 2, 4), Tuple3.of(1, 5, 3));

        DataStream<Tuple3<Integer, Integer, Integer>> resultStream = inputStream
                //.keyBy(0)方法过时，传入KeySelector参数
                .keyBy(r -> r.getField(0))
                .sum(1);

        resultStream.print();

        env.execute();
    }
}
