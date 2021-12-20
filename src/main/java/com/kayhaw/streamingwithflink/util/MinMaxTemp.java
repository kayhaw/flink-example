package com.kayhaw.streamingwithflink.util;

public class MinMaxTemp {

    /** 传感器id */
    public String id;
    /** 最低温度 */
    public Double min;
    /** 最高温度 */
    public Double max;
    /** 窗口结束时间 */
    public Long endTs;

    public MinMaxTemp() { }

    public MinMaxTemp(String id, Double min, Double max, Long endTs) {
        this.id = id;
        this.min = min;
        this.max = max;
        this.endTs = endTs;
    }

    @Override
    public String toString() {
        return "MinMaxTemp{" +
            "id='" + id + '\'' +
            ", min=" + min +
            ", max=" + max +
            ", endTs=" + endTs +
            '}';
    }

}
