# app/main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.config.settings import settings
from app.container.container import container
from app.infrastructure.web.routers import arrow_performance_data

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await container.startup()
    yield
    # Shutdown
    await container.shutdown()

app = FastAPI(
    title=settings.PROJECT_NAME,
    debug=settings.DEBUG,
    version="0.2.2",
    lifespan=lifespan
)

@app.get("/")
async def read_root():
    """
    A simple endpoint to check if the API is running.
    """
    return {"message": "Platform operational", "project_name": settings.PROJECT_NAME}

# 🚀 Arrow-based ultra-fast endpoints
app.include_router(
    arrow_performance_data.router,
    prefix="/api/v1",
    tags=["Arrow Ultra-Fast Operations"]
)