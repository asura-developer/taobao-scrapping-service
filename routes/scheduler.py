from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from services.scheduler_service import scheduler_service
from utils.serializer import clean

router = APIRouter()


class ScheduleBody(BaseModel):
    name: str
    cron: str  # "0 */6 * * *" = every 6 hours
    params: dict  # Same as start_job params: platform, keyword, maxProducts, etc.
    enabled: bool = True


class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    cron: Optional[str] = None
    params: Optional[dict] = None
    enabled: Optional[bool] = None


@router.get("/")
async def list_schedules(request: Request):
    db = request.app.state.db
    schedules = await scheduler_service.list_schedules(db)
    return JSONResponse({"success": True, "data": clean(schedules)})


@router.post("/")
async def create_schedule(request: Request, body: ScheduleBody):
    db = request.app.state.db
    schedule = await scheduler_service.create_schedule(db, body.model_dump())
    return JSONResponse({
        "success": True,
        "data": clean(schedule),
        "message": f"Schedule '{body.name}' created with cron: {body.cron}",
    })


@router.put("/{schedule_id}")
async def update_schedule(request: Request, schedule_id: str, body: ScheduleUpdate):
    db = request.app.state.db
    ok = await scheduler_service.update_schedule(db, schedule_id, body.model_dump(exclude_none=True))
    if not ok:
        raise HTTPException(404, "Schedule not found")
    return JSONResponse({"success": True, "message": "Schedule updated"})


@router.delete("/{schedule_id}")
async def delete_schedule(request: Request, schedule_id: str):
    db = request.app.state.db
    ok = await scheduler_service.delete_schedule(db, schedule_id)
    if not ok:
        raise HTTPException(404, "Schedule not found")
    return JSONResponse({"success": True, "message": "Schedule deleted"})


@router.post("/{schedule_id}/toggle")
async def toggle_schedule(request: Request, schedule_id: str):
    db = request.app.state.db
    new_state = await scheduler_service.toggle_schedule(db, schedule_id)
    if new_state is None:
        raise HTTPException(404, "Schedule not found")
    return JSONResponse({
        "success": True,
        "data": {"enabled": new_state},
        "message": f"Schedule {'enabled' if new_state else 'disabled'}",
    })
