package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Created by freeze on 11/10/15.
 */
@Metrics(context="metrics2")
public class ThemisCpStatistics2 {

    @Metric("An rate in ms")
    public MutableRate commitTotalMetrics;
    // Recommended helper method
    public ThemisCpStatistics2 registerWith(MetricsSystem ms) {
        return ms.register("metrics2", "metrics description", this);
    }
    public static void updateLatency(MutableRate metric, long beginTs) {
        long consumeInUs = (System.nanoTime() - beginTs) / 1000;
        metric.add(consumeInUs);
    }
}
