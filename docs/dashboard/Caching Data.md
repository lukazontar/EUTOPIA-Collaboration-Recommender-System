# Caching Data

## Caching queries with Redis

Because querying BigQuery can be slow and expensive, we can cache the results of queries in Redis. This way, we can
avoid querying BigQuery for the same data multiple times. Redis is an in-memory data store that is very fast and can
store data in key-value pairs.

We use Redis in Plotly Dash to cache the results of queries and user `redis` library to interact with Redis on the
client side.

To install the `redis` library, run the following command:

```shell
pip install redis
```

To run Redis in a Docker container, run the following command:

```shell
docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest
```

To connect to Redis, use the following code snippet:

```python
import redis

redis_url = "redis://localhost:6379"
redis_client = redis.StrictRedis.from_url(redis_url)
```
