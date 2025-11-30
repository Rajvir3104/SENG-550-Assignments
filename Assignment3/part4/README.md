# Part 4: Questions and Answers

## Question 1: Advantages and Disadvantages of Redis Caching vs Direct Model Inference

### Advantages:

1. **Dramatically Faster Response Times**
   - Redis lookup: ~1ms
   - Model inference: ~100-500ms+ per prediction
   - **100-500x faster** for cached predictions

2. **Higher Throughput**
   - Can handle **thousands of requests per second**
   - Model inference might handle 10-100 requests/second
   - Redis can handle 100,000+ operations/second

3. **Lower Computational Cost**
   - No need to load model into memory for each request
   - No CPU/GPU intensive calculations
   - Reduced infrastructure costs

4. **Predictable Latency**
   - Consistent sub-millisecond response times
   - No variance from model computation complexity
   - Better user experience

5. **Reduced Load on ML Infrastructure**
   - Model servers/Spark clusters not hit for every request
   - Can use cheaper hardware for API layer
   - Better resource utilization

### Disadvantages:

1. **Stale Predictions**
   - Predictions are pre-computed, not real-time
   - If data changes, cache must be updated
   - Lag between model retraining and updated predictions

2. **Limited to Pre-computed Combinations**
   - Can only predict for combinations we've cached
   - Cannot handle new categories or arbitrary inputs
   - Must cache all possible combinations (storage overhead)

3. **Memory Overhead**
   - Must store all predictions in Redis
   - For our case: 7 days × 24 hours × N categories = 168N entries
   - Larger feature spaces = exponentially more storage

4. **Cache Invalidation Complexity**
   - Must coordinate between model training and cache updates
   - Risk of serving outdated predictions
   - Need cache versioning strategy

5. **Additional Infrastructure**
   - Redis server adds operational complexity
   - Another component to monitor and maintain
   - Potential single point of failure

## Question 2: Redis Durability and Mitigation Strategies

### Impact if Redis is Cleared:

**Severity: MODERATE - System remains functional but degraded**

#### Immediate Effects:
- All cached predictions lost
- API requests must fall back to direct model inference (if implemented)
- Response times increase from ~1ms to ~100-500ms
- Throughput drops from 100,000+ req/s to ~100 req/s
- Potential request timeouts if traffic is high

#### Duration of Impact:
- Until cache is rebuilt (runs every 20 seconds in our DAG)
- Maximum downtime: 20 seconds
- Gradual recovery as cache repopulates

### Mitigation Strategies:

#### 1. **Redis Persistence (Already Implemented)**
```yaml
redis-cache:
  command: redis-server --appendonly yes  # AOF persistence
```
- Enables Append-Only File (AOF) persistence
- Redis writes every operation to disk
- Survives container restarts
- Trade-off: Slightly slower writes

#### 2. **Fallback to Direct Model Inference**
```python
class ResilientPredictor:
    def __init__(self):
        self.cache = CachedPredictor()
        self.model = load_model()  # Backup model
    
    def predict(self, day, hour, category):
        # Try cache first
        prediction = self.cache.predict(day, hour, category)
        
        if prediction is None:
            # Fall back to model inference
            print("Cache miss - using model")
            prediction = self.model.predict(day, hour, category)
        
        return prediction
```

#### 3. **Redis Replication (Production Setup)**
```yaml
redis-cache-master:
  image: redis:7.2-bookworm
  
redis-cache-replica:
  image: redis:7.2-bookworm
  command: redis-server --replicaof redis-cache-master 6379
```
- Master-replica setup
- If master fails, promote replica
- Near-zero downtime

#### 4. **Cache Warming on Startup**
```python
# In DAG or startup script
def warm_cache_on_startup():
    """Rebuild cache immediately on system start"""
    if redis.dbsize() == 0:
        print("Cache empty - warming cache")
        run_cache_predictions()
```

#### 5. **Monitoring and Alerts**
```python
def check_cache_health():
    metadata = redis.get("cache:metadata")
    if not metadata:
        send_alert("Redis cache is empty!")
    
    last_updated = json.loads(metadata)['last_updated']
    if time.now() - last_updated > timedelta(minutes=5):
        send_alert("Cache is stale!")
```

#### 6. **Multiple Cache Layers**
```python
# L1: Redis (fast, volatile)
# L2: PostgreSQL (slower, durable)
# L3: Model inference (slowest, always available)

def get_prediction_multi_tier(day, hour, category):
    # Try Redis
    pred = redis_cache.get(key)
    if pred: return pred
    
    # Try PostgreSQL
    pred = postgres.query(key)
    if pred:
        redis_cache.set(key, pred)  # Repopulate Redis
        return pred
    
    # Fall back to model
    pred = model.predict(day, hour, category)
    redis_cache.set(key, pred)
    postgres.insert(key, pred)
    return pred
```

#### 7. **Scheduled Cache Backups**
```python
# Daily backup to disk
def backup_redis_to_file():
    all_keys = redis.keys('*')
    backup = {key: redis.get(key) for key in all_keys}
    with open('redis_backup.json', 'w') as f:
        json.dump(backup, f)
```

### Summary:

**Is Redis clearing catastrophic?** No, because:
1. Our DAG rebuilds cache every 20 seconds
2. We have Redis AOF persistence enabled
3. Impact is temporary (20 seconds max)
4. Can implement fallback to direct inference

**Best Practice Stack:**
1. Redis AOF persistence (implemented) ✓
2. Fallback to model inference (recommended)
3. Monitoring/alerts for cache health
4. For production: Redis replication + backup