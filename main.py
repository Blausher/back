from fastapi import FastAPI
import uvicorn

from routers import predict, root

app = FastAPI()

app.include_router(root.router)
app.include_router(predict.router)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8003)
