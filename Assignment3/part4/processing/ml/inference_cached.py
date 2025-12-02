import os
import sys
import redis
import json

# Base directory for part4
BASE_DIR = os.getenv(
    "PROJECT_BASE_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

REDIS_HOST = os.getenv("REDIS_CACHE_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_CACHE_PORT", "6379"))


def get_cached_prediction(day_of_week, hour_of_day, category):
    """Retrieve prediction from Redis."""
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    key = f"{day_of_week}:{hour_of_day}:{category}"
    raw = client.get(key)

    if raw is None:
        raise KeyError(f"No cached value found for key '{key}'")

    data = json.loads(raw)
    return float(data["prediction"])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python inference_cached.py <day> <hour> <category>")
        sys.exit(1)

    dow = int(sys.argv[1])
    hod = int(sys.argv[2])
    category = sys.argv[3]

    try:
        prediction = get_cached_prediction(dow, hod, category)
        print(f"Prediction: {prediction}")
    except KeyError as e:
        print(e)
