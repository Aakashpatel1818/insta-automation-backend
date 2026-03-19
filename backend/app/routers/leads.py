# routers/leads.py
import logging
import csv
import io
from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from bson import ObjectId
from app.dependencies import get_current_user
from app.database import get_db
from app.plans import get_plan_limits

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/leads", tags=["Leads"])


@router.get("/")
async def list_leads(
    account_id: str = Query(...),
    limit: int = Query(default=100, le=500),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    try:
        filt = {"account_id": account_id, "user_id": str(current_user["_id"])}
        leads = await db["leads"].find(filt).sort("captured_at", -1).limit(limit).to_list(length=limit)
        for l in leads:
            l["id"] = str(l.pop("_id"))
        return {"leads": leads, "total": len(leads)}
    except Exception as e:
        logger.error(f"[Leads] list error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{lead_id}")
async def delete_lead(
    lead_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    try:
        result = await db["leads"].delete_one({
            "_id": ObjectId(lead_id),
            "user_id": str(current_user["_id"]),
        })
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Lead not found")
        return {"status": "deleted", "id": lead_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/csv")
async def export_leads_csv(
    account_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    """Export all leads as CSV. Pro and Enterprise plans only."""
    plan   = current_user.get("plan", "free")
    limits = get_plan_limits(plan)
    if not limits["leads_csv_export"]:
        raise HTTPException(
            status_code=403,
            detail=f"CSV export is not available on the {plan} plan. Upgrade to Pro or Enterprise.",
        )

    db = get_db()
    try:
        filt = {"account_id": account_id, "user_id": str(current_user["_id"])}
        leads = await db["leads"].find(filt).sort("captured_at", -1).to_list(length=5000)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "commenter_id", "keyword", "source", "post_id",
        "comment_text", "email", "dm_sent", "reply_sent", "captured_at",
    ])
    for lead in leads:
        writer.writerow([
            lead.get("commenter_id", ""),
            lead.get("keyword", ""),
            lead.get("source", ""),
            lead.get("post_id", ""),
            (lead.get("comment_text") or "").replace("\n", " ")[:300],
            lead.get("email", ""),
            lead.get("dm_sent", False),
            lead.get("reply_sent", False),
            str(lead.get("captured_at", "")),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{account_id}.csv"},
    )


@router.delete("/")
async def clear_leads(
    account_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    result = await db["leads"].delete_many({
        "account_id": account_id,
        "user_id": str(current_user["_id"]),
    })
    return {"status": "cleared", "deleted": result.deleted_count}
