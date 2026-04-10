from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
from services.migration_service import migration_service

router = APIRouter()


class RunBody(BaseModel):
    targetVersion: Optional[int] = None


class RollbackBody(BaseModel):
    targetVersion: int


@router.get("/version")
async def get_version(request: Request):
    db = request.app.state.db
    version = await migration_service.get_current_version(db)
    return {"success": True, "data": {"currentVersion": version}}


@router.post("/run")
async def run_migrations(request: Request, body: RunBody):
    db = request.app.state.db
    result = await migration_service.run_migrations(db, body.targetVersion)
    return {"success": result["success"], "data": result}


@router.post("/rollback")
async def rollback(request: Request, body: RollbackBody):
    db = request.app.state.db
    result = await migration_service.rollback(db, body.targetVersion)
    return {"success": result["success"], "data": result}


@router.post("/cleanup-duplicates")
async def cleanup_duplicates(request: Request):
    db = request.app.state.db
    result = await migration_service.cleanup_duplicates(db)
    return {"success": True, "data": result}