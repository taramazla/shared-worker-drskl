# E-Commerce Stock Update Benchmark

This benchmark simulates a common e-commerce scenario where multiple users attempt to purchase the same product with limited inventory. It demonstrates the importance of proper concurrency control in database systems to prevent race conditions that can lead to overselling.

## Scenario Description

A popular e-commerce site has a limited-edition product (ID 987) with only 50 units in stock. During a flash sale, hundreds of users attempt to purchase this item simultaneously. Without proper concurrency control, this can lead to:

- **Overselling**: More items sold than are available in inventory
- **Negative Stock Values**: Inventory count becoming negative
- **Inconsistent Data**: Expected stock count not matching actual database value

## Benchmark Configuration

The benchmark tests different transaction isolation levels to show their impact on data integrity:

1. **No Transaction Isolation**: Demonstrates race conditions and overselling
2. **READ COMMITTED**: Basic isolation that prevents dirty reads
3. **REPEATABLE READ**: Stronger isolation that also prevents non-repeatable reads
4. **SERIALIZABLE**: Strongest isolation level that prevents all anomalies but may impact performance

## Running the Benchmark

```bash
# Run with different isolation levels
./benchmark/run_ecommerce_benchmark.sh
```

## Results Analysis

After running the benchmark, results are stored in JSON files in the `benchmark_results` directory. Key metrics include:

- **Stock Integrity**: Whether final stock matches expected value
- **Oversold Items**: Number of times stock went negative
- **Successful/Failed Purchases**: Count of purchase attempts
- **Latency**: Response time for purchase and view operations

### Expected Patterns

1. **Without Isolation**: Will likely show overselling and negative stock values
2. **READ COMMITTED**: May still show some inconsistencies in high-concurrency scenarios
3. **REPEATABLE READ**: Should prevent most issues but might still allow some edge cases
4. **SERIALIZABLE**: Should maintain complete data integrity but with higher latency

## Implementation Notes

The benchmark uses the PostgreSQL `SELECT ... FOR UPDATE` pattern with transactions to properly lock rows during stock updates. This is a common pattern for implementing optimistic concurrency control in database applications.

In real-world applications, this would be complemented by:

- Application-level locks
- Retry mechanisms for failed transactions
- Reservation systems for high-demand products
- Monitoring and alerts for inventory discrepancies
