import redis
import json
import hashlib
from typing import Optional, Dict, Any
import os
from datetime import timedelta

class CacheService:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client = None
        self.enabled = os.getenv("ENABLE_CACHE", "true").lower() == "true"
        
        if self.enabled:
            try:
                self.client = redis.from_url(self.redis_url, decode_responses=True)
                self.client.ping()
                print("✅ Redis cache connected")
            except Exception as e:
                print(f"⚠️ Redis connection failed: {e}, caching disabled")
                self.enabled = False
    
    def _get_key(self, prefix: str, query: str) -> str:
        """Generate cache key"""
        query_hash = hashlib.md5(query.encode()).hexdigest()
        return f"{prefix}:{query_hash}"
    
    async def get_cached_query(self, question: str) -> Optional[Dict]:
        """Get cached query result"""
        if not self.enabled:
            return None
        
        try:
            key = self._get_key("query", question)
            cached = self.client.get(key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            print(f"Cache read error: {e}")
        return None
    
    async def cache_query(self, question: str, result: Dict, ttl: int = 3600):
        """Cache query result"""
        if not self.enabled:
            return
        
        try:
            key = self._get_key("query", question)
            self.client.setex(key, timedelta(seconds=ttl), json.dumps(result))
        except Exception as e:
            print(f"Cache write error: {e}")
    
    async def get_cached_graph(self, node_id: str, depth: int) -> Optional[Dict]:
        """Get cached graph subgraph"""
        if not self.enabled or not self.client:
            return None
        
        try:
            key = self._get_key(f"graph:{depth}", node_id)
            cached = self.client.get(key)
            if cached:
                print(f"Cache HIT for {key}")
                return json.loads(cached)
            else:
                print(f"Cache MISS for {key}")
        except Exception as e:
            print(f"Cache read error: {e}")
        return None

    async def cache_graph(self, node_id: str, depth: int, data: Dict, ttl: int = 1800):
        """Cache graph subgraph"""
        if not self.enabled or not self.client:
            return
        
        try:
            key = self._get_key(f"graph:{depth}", node_id)
            self.client.setex(key, timedelta(seconds=ttl), json.dumps(data))
            print(f"Cached {key} for {ttl}s")
        except Exception as e:
            print(f"Cache write error: {e}")
    
    async def invalidate_cache(self, pattern: str = "*"):
        """Invalidate cache by pattern"""
        if not self.enabled:
            return
        
        try:
            keys = self.client.keys(pattern)
            if keys:
                self.client.delete(*keys)
                print(f"Invalidated {len(keys)} cache keys")
        except Exception as e:
            print(f"Cache invalidation error: {e}")

# Create a single instance
cache_service = CacheService()