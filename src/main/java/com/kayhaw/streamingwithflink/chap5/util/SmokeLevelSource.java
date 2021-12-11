package com.kayhaw.streamingwithflink.chap5.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 产生SmokeLevel事件的SourceFunction
 */
public class SmokeLevelSource implements SourceFunction<SmokeLevel> {
    /** 运行标志位 */
    private boolean running = true;

    /** 每秒钟产生一次SmokeLeve事件 */
    @Override
    public void run(SourceContext<SmokeLevel> ctx) throws Exception {
        Random rand = new Random();

        while(running) {

            if(rand.nextGaussian() > 0.8) {
                ctx.collect(SmokeLevel.HIGH);
            } else {
                ctx.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000L);
        }
    }

    /** 取消产生SmokeLevel事件 */
    @Override
    public void cancel() {
        this.running = false;
    }
}
