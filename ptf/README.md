# Window Signal PTF - Cross-Key Windowing Without Watermark Dependency

A Process Table Function (PTF) for Apache Flink on Confluent Cloud that implements windowed aggregations without relying on watermarks, solving watermark stalling issues that occur after interval joins.

## Problem Statement

### The Watermark Stalling Challenge

When building streaming pipelines with Flink SQL on Confluent Cloud, a common pattern involves:

1. **Interval Join**: Join two streams with a time-based lookback window
2. **Windowed Aggregation**: Aggregate the joined results in time windows

However, this creates a critical issue:

```
Stream A ──┐
           ├─► Interval Join ──► Window Aggregation ──► Output
Stream B ──┘        │                    │
                    │                    │
              (lookback causes      (watermark stalls,
               watermark delay)      windows don't close)
```

**The problem**: The interval join's lookback period delays watermarks. Even when new data arrives, the watermark doesn't advance past the lookback window, causing downstream time-based windows to stall indefinitely.


## Solution: Signal-Based Window Flushing

This PTF implements a **signal pattern** where any incoming record crossing a window boundary triggers output for **all accumulated keys**, regardless of which keys are present in the next window.

### Key Innovation

Unlike traditional keyed operations where each key's state is managed independently, this PTF:

1. **Shares window boundaries across all keys**: All keys use the same time-based window definition
2. **Cross-key flushing**: When ANY record signals a window change, ALL keys from the previous window are flushed
3. **Watermark independence**: Uses event time values directly, not watermark progression
4. **Global sync key pattern**: Uses a constant partition key (e.g., `'global_sync'`) to route all records to the same operator instance, enabling cross-key coordination

## How It Works

### Architecture

```
Input Records (from Interval Join)
     │
     ▼
┌────────────────────────────────────┐
│  WindowSignalPTF                   │
│                                    │
│  Shared State:                     │
│  ┌──────────────────────────────┐ │
│  │ nextWindowEnd: 2024-01-01    │ │
│  │   00:01:00.000               │ │
│  │                              │ │
│  │ amountBuckets:               │ │
│  │   order_101 → 250.0          │ │
│  │   order_104 → 180.0          │ │
│  │   order_107 → 420.0          │ │
│  └──────────────────────────────┘ │
└────────────────────────────────────┘
     │
     │ Signal: Record with time >= 00:01:00
     ▼
Flush ALL 3 orders (101, 104, 107)
Clear state, advance to next window
```

### Window Signal Logic

**1. Window Boundary Initialization**
```java
if (state.nextWindowEnd == null) {
    state.nextWindowEnd = (rowTime / 60000 + 1) * 60000;
}
```
Sets the first window boundary (1-minute windows by default).

**2. Signal Detection & Cross-Key Flush**
```java
if (rowTime >= state.nextWindowEnd) {
    // Flush EVERY key in the map
    for (Map.Entry<Integer, Double> entry : state.amountBuckets.entrySet()) {
        if (entry.getValue() > 0) {
            collect(Row.of(
                entry.getKey(),
                state.nextWindowEnd - 60000,
                state.nextWindowEnd,
                entry.getValue()
            ));
        }
    }
    // Clear state and advance window
    state.amountBuckets.clear();
    state.nextWindowEnd = (rowTime / 60000 + 1) * 60000;
}
```

**3. Accumulation**
```java
if (amount != null && amount > 0) {
    Double currentSum = state.amountBuckets.getOrDefault(orderId, 0.0);
    state.amountBuckets.put(orderId, currentSum + amount);
}
```

### Example Scenario

**Input Data (with sync_key)**
All records have `sync_key = 'global_sync'` to route to the same partition:

**Window 1 (00:00:00 - 00:01:00)**
- Record arrives: `order_101, amount=100.0, time=00:00:15, sync_key='global_sync'` → Accumulate
- Record arrives: `order_104, amount=180.0, time=00:00:30, sync_key='global_sync'` → Accumulate
- Record arrives: `order_101, amount=150.0, time=00:00:45, sync_key='global_sync'` → Accumulate (total: 250.0)

**Window Transition**
- Record arrives: `order_101, amount=50.0, time=00:01:05, sync_key='global_sync'` → **SIGNAL DETECTED**
- **Flush ALL orders from window 1:**
  - Output: `(order_101, 0, 60000, 250.0)` → Formatted: `(101, '00:00:00', '00:01:00', 250.0)`
  - Output: `(order_104, 0, 60000, 180.0)` → Formatted: `(104, '00:00:00', '00:01:00', 180.0)`
- Clear state, start accumulating for window 2

**Critical**: Even though `order_104` has no records in window 2, it still gets flushed when window 2 starts. This is the "irrespective of key" behavior enabled by the `sync_key` pattern.

## Implementation Details

### State Management

**WindowState.java**
```java
public class WindowState implements Serializable {
    // Map of order_id → aggregated amount for current window
    public Map<Integer, Double> amountBuckets = new HashMap<>();
    
    // Next window boundary timestamp (milliseconds)
    public Long nextWindowEnd = null;
}
```

### Output Schema

Defined via class-level annotation:
```java
@DataTypeHint("ROW<order_id INT, window_start BIGINT, window_end BIGINT, total_sum DOUBLE>")
public class WindowSignalPTF extends ProcessTableFunction<Row>
```

### Input Processing

Uses `@ArgumentHint({SET_SEMANTIC_TABLE})` to accept any input table schema. The PTF expects:
- **Field 0**: `order_id` (INTEGER)
- **Field 1**: `amount` (DOUBLE)
- **Field 2**: `rtime` (TIMESTAMP - event time)

## Usage

### Building the JAR

```bash
mvn clean package
```

This produces `target/window-signal-ptf-1.0.jar` - an uber-jar with all dependencies.

### Uploading to Confluent Cloud

```bash
# Upload the JAR to your Confluent Cloud Flink environment
confluent flink udf create \
  --artifact-file target/window-signal-ptf-1.0.jar \
  --name window-signal-ptf
```

### SQL Integration

**Step 1: Register the Function**

```sql
CREATE FUNCTION windowptf 
AS 'io.confluent.flink.examples.windowstate.WindowSignalPTF' 
USING JAR 'confluent-artifact://<id>';
```

**Step 2: Use in Your Pipeline**

The key pattern is to use a **global sync key** to ensure all records are processed together, enabling cross-key window flushing:

```sql
-- Complete working example with interval join
WITH real_data AS (
    SELECT 
        o.order_id, 
        o.amount, 
        o.`$rowtime` as rtime,
        'global_sync' as sync_key  -- Constant key to process all records together
    FROM orders o
    JOIN payments p 
        ON o.order_id = p.order_id
    WHERE p.`$rowtime` BETWEEN o.`$rowtime` - INTERVAL '10' MINUTE 
                           AND o.`$rowtime`
)
SELECT 
    order_id,
    FROM_UNIXTIME(window_start / 1000, 'yyyy-MM-dd HH:mm:ss') AS readable_start,
    FROM_UNIXTIME(window_end / 1000, 'yyyy-MM-dd HH:mm:ss') AS readable_end,
    total_sum
FROM TABLE(
    windowptf(
        input => (SELECT * FROM real_data) 
                 PARTITION BY sync_key
    )
)
WHERE total_sum > 0;
```

**Key Points:**

1. **`sync_key` pattern**: Use a constant value (e.g., `'global_sync'`) to route all records to the same partition, enabling the signal pattern to flush all keys together

2. **TABLE() syntax**: The PTF must be called using `TABLE()` with the named parameter `input =>`

3. **PARTITION BY sync_key**: Ensures all records go to one partition where cross-key flushing works

4. **Interval Join**: The example shows the actual use case - an interval join between `orders` and `payments` with a 10-minute lookback that causes watermark stalling

5. **Output formatting**: Use `FROM_UNIXTIME()` to convert millisecond timestamps to readable format

**Example 2: Regional Partitioning**

For parallel processing across regions while maintaining cross-key flushing within each region:

```sql
WITH regional_data AS (
    SELECT 
        o.order_id,
        o.amount,
        o.`$rowtime` as rtime,
        o.region_id as sync_key  -- Use region as partition key
    FROM orders o
    JOIN payments p 
        ON o.order_id = p.order_id
    WHERE p.`$rowtime` BETWEEN o.`$rowtime` - INTERVAL '10' MINUTE 
                           AND o.`$rowtime`
)
SELECT 
    order_id,
    FROM_UNIXTIME(window_start / 1000, 'yyyy-MM-dd HH:mm:ss') AS window_start_time,
    FROM_UNIXTIME(window_end / 1000, 'yyyy-MM-dd HH:mm:ss') AS window_end_time,
    total_sum
FROM TABLE(
    windowptf(
        input => (SELECT * FROM regional_data)
                 PARTITION BY sync_key
    )
)
WHERE total_sum > 0;
```

**Example 3: Inserting Results into a Table**

```sql
-- Create output table
CREATE TABLE windowed_order_totals (
    order_id INT,
    window_start_time STRING,
    window_end_time STRING,
    total_sum DOUBLE
);

-- Insert PTF results
INSERT INTO windowed_order_totals
WITH real_data AS (
    SELECT 
        o.order_id, 
        o.amount, 
        o.`$rowtime` as rtime,
        'global_sync' as sync_key
    FROM orders o
    JOIN payments p 
        ON o.order_id = p.order_id
    WHERE p.`$rowtime` BETWEEN o.`$rowtime` - INTERVAL '10' MINUTE 
                           AND o.`$rowtime`
)
SELECT 
    order_id,
    FROM_UNIXTIME(window_start / 1000, 'yyyy-MM-dd HH:mm:ss') AS window_start_time,
    FROM_UNIXTIME(window_end / 1000, 'yyyy-MM-dd HH:mm:ss') AS window_end_time,
    total_sum
FROM TABLE(
    windowptf(
        input => (SELECT * FROM real_data)
                 PARTITION BY sync_key
    )
)
WHERE total_sum > 0;
```

## Customization

### Changing Window Size

Modify the window calculation in `WindowSignalPTF.java`:

```java
// 1-minute windows (current)
state.nextWindowEnd = (rowTime / 60000 + 1) * 60000;

// 5-minute windows
state.nextWindowEnd = (rowTime / 300000 + 1) * 300000;

// 1-hour windows
state.nextWindowEnd = (rowTime / 3600000 + 1) * 3600000;
```

### Different Input Schema

Adjust field indices in the `eval()` method:

```java
// Current: order_id, amount, rtime
Integer orderId = (Integer) input.getField(0);
Double amount = (Double) input.getField(1);
Instant rtime = (Instant) input.getField(2);

// Example: customer_id, order_id, revenue, event_time
String customerId = (String) input.getField(0);
Integer orderId = (Integer) input.getField(1);
Double revenue = (Double) input.getField(2);
Instant eventTime = (Instant) input.getField(3);
```

### Different Aggregation Logic

Replace the simple sum with other aggregations:

```java
// Count distinct orders
state.amountBuckets.put(orderId, 1.0);

// Max value per order
Double currentMax = state.amountBuckets.getOrDefault(orderId, Double.MIN_VALUE);
state.amountBuckets.put(orderId, Math.max(currentMax, amount));

// Average (store sum and count separately)
// Would require modifying WindowState to track counts
```

## Requirements

- **Java**: 17
- **Flink Table API**: 2.1.0 (provided by Confluent Cloud)
- **Maven**: 3.6+
- **Confluent Cloud Flink**: Environment with PTF support


### Memory Considerations

State grows with the number of distinct keys per window. For high-cardinality scenarios:

- Use `PARTITION BY` to distribute load
- Consider state TTL if implementing long windows
- Monitor state size in production

### Window Semantics

This implements **tumbling windows** only. For:
- **Sliding windows**: Multiple overlapping PTF instances
- **Session windows**: Requires additional gap-tracking logic

## When to Use This PTF

✅ **Use when:**
- You have watermark stalling after interval joins
- You need windows but can't rely on watermark progression
- Processing time operations aren't available
- You want cross-key coordination in window closure

❌ **Don't use when:**
- Watermarks advance normally (use standard window TVFs)
- You need perfect late-data handling (use event-time windows with allowed lateness)
- Processing time windows are sufficient and available

## References

- [Confluent Cloud Flink PTF Documentation](https://docs.confluent.io/cloud/current/flink/reference/functions/process-table-functions.html)
- [Reference PTF Example](https://github.com/nicusX/cc-flink-udf-examples/blob/main/src/main/java/io/confluent/flink/examples/ptf/statemachine/EntityStateMachine.java)
- [Flink Process Table Functions](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/process-table-functions/)

## License

This example is provided as-is for educational and development purposes.

## Contributing

Feel free to open issues or submit pull requests for:
- Bug fixes
- Performance improvements
- Additional windowing strategies
- Better late-data handling

---

**Built for Confluent Cloud Flink** | **Solves Watermark Stalling** | **Cross-Key Window Coordination**
