package com.kayhaw.streamingwithflink.chap5.util;

/**
 * 温度警报POJO
 */
public class Alert {
    public String message;
    public long timestamp;

    /** Flink要求POJO提供一个默认空参构造器 */
    public Alert() { }

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "(" + message + ", " + timestamp + ")";
    }
}
