import logging

from fastapi import FastAPI
import uvicorn

from app.routers import entities, predict, root


logging.basicConfig(level=logging.INFO)

app = FastAPI()

app.include_router(root.router)
app.include_router(predict.router)
app.include_router(entities.router)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8003)
