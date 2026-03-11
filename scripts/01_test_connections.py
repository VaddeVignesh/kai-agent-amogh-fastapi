import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.config.database import get_mongo_db, get_postgres_connection, get_redis_client

# Test MongoDB
get_mongo_db().client.server_info()
print("✅ MongoDB connected")

# Test PostgreSQL
conn = get_postgres_connection()
conn.close()
print("✅ PostgreSQL connected")

# Test Redis
get_redis_client().ping()
print("✅ Redis connected")
