from fastapi import Request
from fastapi.responses import JSONResponse
import time
from collections import defaultdict
import asyncio

class RateLimiter:
    def __init__(self, requests_per_minute: int = 30):
        self.requests_per_minute = requests_per_minute
        self.requests = defaultdict(list)
    
    async def check_rate_limit(self, request: Request) -> tuple[bool, int]:
        """Check rate limit, returns (allowed, retry_after_seconds)"""
        client_ip = request.client.host
        now = time.time()
        minute_ago = now - 60
        
        # Clean old requests
        self.requests[client_ip] = [req_time for req_time in self.requests[client_ip] if req_time > minute_ago]
        
        # Check limit
        if len(self.requests[client_ip]) >= self.requests_per_minute:
            # Calculate when the oldest request will expire
            oldest = min(self.requests[client_ip])
            retry_after = int(60 - (now - oldest))
            return False, max(1, retry_after)
        
        # Add current request
        self.requests[client_ip].append(now)
        return True, 0

rate_limiter = RateLimiter()

async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware"""
    # Skip rate limiting for health check and static files
    skip_paths = ["/health", "/", "/docs", "/openapi.json", "/api/debug/"]
    if any(request.url.path.startswith(path) for path in skip_paths):
        return await call_next(request)
    
    # Check rate limit
    is_allowed, retry_after = await rate_limiter.check_rate_limit(request)
    
    if not is_allowed:
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": str(retry_after)},
            content={
                "error": "Rate limit exceeded",
                "message": f"Too many requests. Please wait {retry_after} seconds and try again.",
                "retry_after": retry_after,
                "limit": "30 requests per minute"
            }
        )
    
    return await call_next(request)