from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI
import uvicorn

from app.routers import entities, predict, root
from app.services.model import load_or_train_model


@asynccontextmanager
async def lifespan(app: FastAPI):
    model = getattr(app.state, "model", None)
    # In tests model can be injected explicitly before first request.
    if model is None and "PYTEST_CURRENT_TEST" not in os.environ:
        app.state.model = load_or_train_model("model.pkl")

    yield


logging.basicConfig(level=logging.INFO)

app = FastAPI(lifespan=lifespan)

app.include_router(root.router)
app.include_router(predict.router)
app.include_router(entities.router)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8003)
