package io.confluent.flink.examples.windowstate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class WindowState implements Serializable {
    // Stores sum for each order_id for the CURRENT window
    public Map<Integer, Double> amountBuckets = new HashMap<>();
    public Long nextWindowEnd = null;

    public WindowState() {}
}