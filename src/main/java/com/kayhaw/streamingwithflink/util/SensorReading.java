package com.kayhaw.streamingwithflink.util;

/**
 * 传感器数据POJO
 */
public class SensorReading {

    /** 传感器id */
    public String id;

    /** 读取时间戳 */
    public long timestamp;

    /** 读取温度 */
    public double temperature;

    /** Flink要求POJO提供一个默认空参构造器 */
    public SensorReading() { }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }

}
