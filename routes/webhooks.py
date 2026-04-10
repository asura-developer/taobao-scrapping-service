from typing import Optional
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from services.webhook_service import create_webhook, list_webhooks, delete_webhook, toggle_webhook
from utils.serializer import clean

router = APIRouter()


class WebhookBody(BaseModel):
    name: str
    url: str
    events: list[str] = ["*"]  # ["job.completed", "job.failed", "price.drop"] or ["*"]
    enabled: bool = True


@router.get("/")
async def get_webhooks(request: Request):
    db = request.app.state.db
    webhooks = await list_webhooks(db)
    return JSONResponse({"success": True, "data": clean(webhooks)})


@router.post("/")
async def add_webhook(request: Request, body: WebhookBody):
    db = request.app.state.db
    webhook = await create_webhook(db, body.model_dump())
    return JSONResponse({"success": True, "data": clean(webhook), "message": "Webhook created"})


@router.delete("/{webhook_id}")
async def remove_webhook(request: Request, webhook_id: str):
    db = request.app.state.db
    ok = await delete_webhook(db, webhook_id)
    if not ok:
        raise HTTPException(404, "Webhook not found")
    return JSONResponse({"success": True, "message": "Webhook deleted"})


@router.post("/{webhook_id}/toggle")
async def toggle(request: Request, webhook_id: str):
    db = request.app.state.db
    new_state = await toggle_webhook(db, webhook_id)
    if new_state is None:
        raise HTTPException(404, "Webhook not found")
    return JSONResponse({"success": True, "data": {"enabled": new_state}})
