package com.kayhaw.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @deprecated 使用WatermarkStrategy代替
 * @see org.apache.flink.api.common.eventtime.WatermarkStrategy
 */
public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

    /**
     * Configures the extractor with 5 seconds out-of-order interval.
     */
    public SensorTimeAssigner() {
        super(Time.seconds(5));
    }

    /**
     * Extracts timestamp from SensorReading.
     *
     * @param r sensor reading
     * @return the timestamp of the sensor reading.
     */
    @Override
    public long extractTimestamp(SensorReading r) {
        return r.timestamp;
    }
}