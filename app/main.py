import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from app.clients.kafka import kafka_client
from app.clients.postgres import close_pg_pool, init_pg_pool
from app.routers import entities, predict, root
from app.observability.middleware import PrometheusMiddleware


logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_pg_pool()
    await kafka_client.start()
    try:
        yield
    finally:
        await kafka_client.stop()
        await close_pg_pool()


app = FastAPI(lifespan=lifespan)

app.include_router(root.router)
app.include_router(predict.router)
app.include_router(entities.router)

app.add_middleware(PrometheusMiddleware)



if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8003)
