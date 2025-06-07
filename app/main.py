# app/main.py
from fastapi import FastAPI
from app.config.settings import settings

app = FastAPI(
    title=settings.APP_NAME,
    debug=settings.DEBUG_MODE,
    version="0.1.0" # Optional: Add a version
)

@app.get("/")
async def read_root():
    """
    A simple endpoint to check if the API is running.
    """
    return {"message": "Platform operational", "app_name": settings.APP_NAME}

# Later, we will include routers here in Sprint 2:
# from src.infrastructure.web.routers import data
# app.include_router(data.router, prefix="/api", tags=["data"]) # Example