from fastapi import FastAPI
import uvicorn
import os

from mangum import Mangum
from fastapi.middleware.cors import CORSMiddleware


root_path = os.getenv('ENV', default='dev')

app = FastAPI(root_path=f'/{root_path}')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=["*"],
    allow_headers=["*"],
)

print("Here!")

# The magic that allows the integration with AWS Lambda
handler = Mangum(
    app, 
    lifespan="off", 
    api_gateway_base_path=root_path
)


if __name__ == "__main__":
    uvicorn.run(app, port=8000)