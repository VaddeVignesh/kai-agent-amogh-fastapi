from __future__ import annotations

import os

from pymongo import MongoClient
import psycopg2
import redis

def get_mongo_db():
    """
    Return a PyMongo Database handle.

    Uses env vars when present:
    - MONGO_URI (default: mongodb://localhost:27017/)
    - MONGO_DB_NAME (default: kai_agent)

    Adds short timeouts so the chatbot fails fast when Mongo is down.
    """
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/").strip()
    db_name = os.getenv("MONGO_DB_NAME", "kai_agent").strip() or "kai_agent"
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "800")),
        connectTimeoutMS=int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "800")),
        socketTimeoutMS=int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "1200")),
    )
    return client[db_name]

def get_postgres_connection():
    """
    Legacy helper. Prefer PostgresAdapter, but keep this for scripts.
    Adds connect_timeout so failures return quickly.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin123"),
        database=os.getenv("POSTGRES_DB", "postgres"),
        connect_timeout=int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SEC", "2")),
    )

def get_redis_client():
    """
    Legacy helper. Prefer RedisStore, but keep this for scripts.
    Adds short socket timeouts so failures return quickly.
    """
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
        socket_connect_timeout=float(os.getenv("REDIS_CONNECT_TIMEOUT_SEC", "0.8")),
        socket_timeout=float(os.getenv("REDIS_SOCKET_TIMEOUT_SEC", "0.8")),
    )
