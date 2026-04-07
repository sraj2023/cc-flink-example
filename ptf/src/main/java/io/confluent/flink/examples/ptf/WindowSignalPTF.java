package io.confluent.flink.examples.ptf;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Map;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

/**
 * This PTF implements a window-based aggregation pattern without watermark dependency,
 * using a signal pattern where any record crossing a window boundary triggers output
 * for ALL accumulated keys.
 * <p>
 * This solves the watermark stalling issue that occurs after interval joins in Confluent Cloud Flink,
 * where the interval join's lookback period delays watermarks and prevents downstream windows from closing.
 * <p>
 * The PTF processes input records containing order_id, amount, and event timestamp (rtime).
 * It accumulates amounts per order_id within 1-minute tumbling windows, and flushes ALL orders
 * when ANY record signals a window transition.
 * <p>
 * Example SQL usage:
 * <code>
 * WITH real_data AS (
 *     SELECT o.order_id, o.amount, o.$rowtime as rtime, 'global_sync' as sync_key
 *     FROM orders o
 *     JOIN payments p ON o.order_id = p.order_id
 *     WHERE p.$rowtime BETWEEN o.$rowtime - INTERVAL '10' MINUTE AND o.$rowtime
 * )
 * SELECT order_id, window_start, window_end, total_sum
 * FROM TABLE(windowptf(input => (SELECT * FROM real_data) PARTITION BY sync_key))
 * WHERE total_sum > 0;
 * </code>
 * <p>
 * Note: This PTF logs on every processed record for debugging purposes. In a production scenario,
 * it is advisable to reduce logging to WARN or ERROR level to avoid performance impact.
 */
@DataTypeHint("ROW<order_id INT, window_start BIGINT, window_end BIGINT, total_sum DOUBLE>")
public class WindowSignalPTF extends ProcessTableFunction<Row> {
    private static final Logger LOG = LogManager.getLogger(WindowSignalPTF.class);

    public void eval(
            @StateHint WindowState state,
            @ArgumentHint({SET_SEMANTIC_TABLE}) Row input) {

        // Extract fields from input row
        Integer orderId = (Integer) input.getField(0);
        Double amount = (Double) input.getField(1);
        Instant rtime = (Instant) input.getField(2);

        if (rtime == null || orderId == null) {
            LOG.warn("Skipping record with null orderId or timestamp");
            return;
        }
        long rowTime = rtime.toEpochMilli();

        LOG.info("Processing record for order {} with amount {} at timestamp {}", orderId, amount, rtime);

        // Initialize window boundary on first record
        if (state.nextWindowEnd == null) {
            state.nextWindowEnd = (rowTime / 60000 + 1) * 60000;
            LOG.info("Initialized first window boundary to {}", Instant.ofEpochMilli(state.nextWindowEnd));
        }

        // Signal detected: window boundary crossed
        if (rowTime >= state.nextWindowEnd) {
            LOG.info("Window boundary crossed! Current time {} >= window end {}. Flushing {} orders.",
                    rtime, Instant.ofEpochMilli(state.nextWindowEnd), state.amountBuckets.size());

            // Flush ALL accumulated orders (cross-key flush)
            for (Map.Entry<Integer, Double> entry : state.amountBuckets.entrySet()) {
                if (entry.getValue() > 0) {
                    LOG.info("Emitting window result for order {} with total {}",
                            entry.getKey(), entry.getValue());
                    collect(Row.of(
                        entry.getKey(),
                        state.nextWindowEnd - 60000,
                        state.nextWindowEnd,
                        entry.getValue()
                    ));
                }
            }

            // Clear state and advance to next window
            state.amountBuckets.clear();
            long previousWindowEnd = state.nextWindowEnd;
            state.nextWindowEnd = (rowTime / 60000 + 1) * 60000;
            LOG.info("Advanced window from {} to {}",
                    Instant.ofEpochMilli(previousWindowEnd),
                    Instant.ofEpochMilli(state.nextWindowEnd));
        }

        // Accumulate amount for this order in the current window
        if (amount != null && amount > 0) {
            Double currentSum = state.amountBuckets.getOrDefault(orderId, 0.0);
            Double newSum = currentSum + amount;
            state.amountBuckets.put(orderId, newSum);
            LOG.info("Accumulated order {}: previous sum = {}, added = {}, new sum = {}",
                    orderId, currentSum, amount, newSum);
        } else {
            LOG.debug("Skipping accumulation for order {} with amount {}", orderId, amount);
        }
    }
}