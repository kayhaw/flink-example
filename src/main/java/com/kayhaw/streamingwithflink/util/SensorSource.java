package com.kayhaw.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    // 运行标志位
    private boolean running = true;

    /** run()方法通过SourceContext连续地发送传感器数据 */
    @Override
    public void run(SourceContext<SensorReading> srcCtx) throws Exception {

        // 初始化随机数生成器
        Random rand = new Random();
        // 查看当前并发任务的编号
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        // 初始化传感器id和温度
        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + (taskIdx * 10 + i);
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {

            // 获取当前时间
            long curTime = Calendar.getInstance().getTimeInMillis();

            // 发送传感器数据
            for (int i = 0; i < 10; i++) {
                // 更新温度
                curFTemp[i] += rand.nextGaussian() * 0.5;
                // 输出
                srcCtx.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
            }

            // 每隔100ms生产一条记录
            Thread.sleep(100);
        }
    }

    /** 停止产生数据 */
    @Override
    public void cancel() {
        this.running = false;
    }
}
