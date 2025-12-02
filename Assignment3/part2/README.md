## Part 2 – Redis Tracking Logic

In Part 2 I use Redis just to keep track of **how far the incremental job has gone**.  
The real data still lives on disk in the raw folders and in the aggregated CSV.

The idea is:

- I keep a key in Redis (for example `incremental:last_processed_day`).
- When the Spark job starts, it:
  1. Reads this key from Redis. If it doesn’t exist, I assume nothing is processed yet.
  2. Looks under `data/incremental/raw/{dow}/` and finds all day-of-week folders that are
     **greater than** the last processed day.
  3. Loads only those “new” days and aggregates them with Spark.
  4. Appends the new aggregated rows into `data/processed/orders.csv`.
  5. Updates the Redis key to the highest day index it just processed.

Because of this, each Airflow run only processes new raw data instead of re-reading
everything every time. Redis is basically a lightweight checkpoint telling the job
“you already finished up to this day.”



## Does losing Redis data hurt the system? Why or why not?

Redis is not durable by default, so if it crashes or gets cleared, we lose the key that
stores the last processed day for the incremental job. This does not delete any real
data, because the actual raw files and the aggregated output CSV are stored on disk.
However, losing the Redis key can still cause problems depending on how the system
handles the restart.

If the incremental Spark job relies only on Redis to decide what has already been
processed, then a Redis reset means the job will think nothing has been done yet and
reprocess all raw days again. If the code simply appends, this will lead to
duplicate rows and inconsistent aggregates. So yes, this can hurt the system if the
pipeline is not designed to handle this situation.

## How can this issue be mitigated?

There are a few ways to avoid inconsistencies:

1. **Make the pipeline idempotent**  
   The job should be safe to rerun. Instead of blindly appending new results, it can
   overwrite existing partitions or rebuild the entire output. Then it doesn’t matter
   if Redis resets — the output stays correct.

2. **Do not rely on Redis as the source of truth**  
   Redis should only be used as an optimization. If Redis is empty, the job can look at
   the processed folder and infer which days were already computed.

3. **Enable Redis persistence (RDB/AOF)**  
   In a more realistic system, enabling Redis persistence will write the key to disk.
   This reduces the chance of losing the last processed day after a crash.

4. **Store progress in a more durable place**  
   A small metadata file (e.g., a JSON file) or a simple database row can track the last
   processed day. Redis can still be used for fast access, but durability is guaranteed
   elsewhere.

