import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from .metrics import REQUEST_COUNT, REQUEST_DURATION

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        '''точка входа для каждого http запроса'''
        path = request.url.path 
        # Don't self-instrument Prometheus scrape
        if path == "/metrics":
            return await call_next(request)
        
        method = request.method
        endpoint = request.url.path
        start_time = time.time()

        response = await call_next(request)

        duration = time.time() - start_time
        REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=response.status_code).inc()
        REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

        return response

