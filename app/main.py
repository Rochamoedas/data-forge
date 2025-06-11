# app/main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn

from app.config.settings import settings
from app.container.container import container
from app.infrastructure.web.routers import data_router, arrow_performance_data
from app.config.logging_config import logger

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages application startup and shutdown events.
    """
    logger.info("Application starting up...")
    await container.startup()
    yield
    logger.info("Application shutting down...")
    await container.shutdown()
    logger.info("Application shutdown complete.")

# Create the FastAPI app instance
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="A high-performance, schema-driven data platform.",
    version="2.0.0",
    lifespan=lifespan
)

# Mount the API routers
app.include_router(arrow_performance_data.router, prefix="/api/v1")
app.include_router(data_router.router, prefix="/api/v1")

@app.get("/", tags=["Root"])
def read_root():
    """A welcome message to verify the service is running."""
    return {"message": "Welcome to the Data Forge API"}

if __name__ == "__main__":
    """
    Allows running the app directly with uvicorn for development.
    """
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=True
    )