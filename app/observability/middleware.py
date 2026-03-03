from time import perf_counter

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from .metrics import REQUEST_COUNT, REQUEST_DURATION

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        '''точка входа для каждого http запроса'''
        path = request.url.path 
        # Don't self-instrument Prometheus scrape
        if path == "/metrics":
            return await call_next(request)
        
        method = request.method
        start_time = perf_counter()

        response = await call_next(request)

        route = request.scope.get("route")
        endpoint = getattr(route, "path", path)
        duration = perf_counter() - start_time

        REQUEST_COUNT.labels(
            method=method,
            endpoint=endpoint,
            status=str(response.status_code),
        ).inc()
        REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

        return response
