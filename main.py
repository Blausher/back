from fastapi import FastAPI
from routers.users import router as user_router

app = FastAPI()

@app.get('/')
async def root():
    return {'message': 'Hello World'}
