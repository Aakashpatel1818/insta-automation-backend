import logging
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from bson import ObjectId

from app.dependencies import get_current_user
from app.database import get_db
from app.automation.models import (
    AutomationSettingsRequest,
    KeywordRuleRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/automation", tags=["Automation"])


# ── Helper ────────────────────────────────────────────────
async def verify_account_ownership(db, account_id: str, user_id: str):
    try:
        account = await db["instagram_accounts"].find_one({
            "_id":     ObjectId(account_id),
            "user_id": user_id,
        })
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid account ID")
    if not account:
        raise HTTPException(status_code=404, detail="Instagram account not found")
    return account


# ═══════════════════════════════════════════════════════════
# AUTOMATION SETTINGS
# ═══════════════════════════════════════════════════════════

@router.post("/settings", status_code=status.HTTP_201_CREATED)
async def create_or_update_settings(
    body: AutomationSettingsRequest,
    current_user: dict = Depends(get_current_user),
):
    """Create a NEW automation settings document (always inserts, never merges)."""
    db = get_db()
    await verify_account_ownership(db, body.account_id, str(current_user["_id"]))

    now = datetime.utcnow()
    doc = {
        "user_id":            str(current_user["_id"]),
        "post_id":            body.post_id,
        "account_id":         body.account_id,
        "auto_comment_reply": body.auto_comment_reply,
        "auto_dm":            body.auto_dm,
        "delay_enabled":      body.delay_enabled,
        "is_active":          body.is_active,
        "created_at":         now,
        "updated_at":         now,
    }
    result = await db["automation_settings"].insert_one(doc)
    automation_id = str(result.inserted_id)
    logger.info(f"Automation settings created: {automation_id} for post {body.post_id}")
    return {
        "status":        "created",
        "automation_id": automation_id,
        "post_id":       body.post_id,
    }


@router.get("/settings/{post_id}")
async def get_settings(
    post_id: str,
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    settings = await db["automation_settings"].find_one({
        "post_id":    post_id,
        "account_id": account_id,
        "user_id":    str(current_user["_id"]),
    })
    if not settings:
        raise HTTPException(status_code=404, detail="No automation settings found for this post")
    settings["id"] = str(settings.pop("_id"))
    return settings


@router.delete("/settings/{post_id}")
async def delete_settings(
    post_id: str,
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    result = await db["automation_settings"].delete_one({
        "post_id":    post_id,
        "account_id": account_id,
        "user_id":    str(current_user["_id"]),
    })
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Settings not found")
    return {"status": "deleted", "post_id": post_id}


# ═══════════════════════════════════════════════════════════
# KEYWORD RULES
# ═══════════════════════════════════════════════════════════

@router.post("/rules", status_code=status.HTTP_201_CREATED)
async def create_rule(
    body: KeywordRuleRequest,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    await verify_account_ownership(db, body.account_id, str(current_user["_id"]))

    now = datetime.utcnow()
    doc = {
        "user_id":                     str(current_user["_id"]),
        "post_id":                     body.post_id,
        "account_id":                  body.account_id,
        "automation_id":               body.automation_id if hasattr(body, "automation_id") else None,
        "trigger_words":               [w.lower().strip() for w in body.trigger_words],
        "response":                    body.response,
        "reply_comment":               body.reply_comment,
        "send_dm":                     body.send_dm,
        "is_active":                   body.is_active,
        "opening_message":         getattr(body, "opening_message", ""),
        "opening_message_btn":     getattr(body, "opening_message_btn", ""),
        "opening_message_btn_url": getattr(body, "opening_message_btn_url", ""),
        "follow_dm_message":       getattr(body, "follow_dm_message", ""),
        "dm_actions":              getattr(body, "dm_actions", []),
        "created_at":              now,
        "updated_at":              now,
    }
    result = await db["keyword_rules"].insert_one(doc)
    logger.info(f"Keyword rule created for post {body.post_id}")
    return {
        "status":  "created",
        "rule_id": str(result.inserted_id),
        "post_id": body.post_id,
    }


@router.get("/rules/{post_id}")
async def get_rules(
    post_id: str,
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    rules = await db["keyword_rules"].find({
        "post_id":    post_id,
        "account_id": account_id,
        "user_id":    str(current_user["_id"]),
    }).to_list(length=100)
    for r in rules:
        r["id"] = str(r.pop("_id"))
    return {"rules": rules, "total": len(rules)}


@router.put("/rules/{rule_id}")
async def update_rule(
    rule_id: str,
    body: KeywordRuleRequest,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    try:
        oid = ObjectId(rule_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid rule ID")

    result = await db["keyword_rules"].update_one(
        {"_id": oid, "user_id": str(current_user["_id"])},
        {"$set": {
            "trigger_words":               [w.lower().strip() for w in body.trigger_words],
            "response":                    body.response,
            "reply_comment":               body.reply_comment,
            "send_dm":                     body.send_dm,
            "is_active":                   body.is_active,
            "updated_at": datetime.utcnow(),
        }}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"status": "updated", "rule_id": rule_id}


@router.delete("/rules/{rule_id}")
async def delete_rule(
    rule_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    try:
        oid = ObjectId(rule_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid rule ID")
    result = await db["keyword_rules"].delete_one({
        "_id":     oid,
        "user_id": str(current_user["_id"]),
    })
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"status": "deleted", "rule_id": rule_id}


# ═══════════════════════════════════════════════════════════
# LIST / DETAIL / EDIT / DELETE AUTOMATION
# ═══════════════════════════════════════════════════════════

@router.get("/list/{account_id}")
async def list_automations(
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    List every automation as a SEPARATE row — one per automation_settings document.
    Two automations on the same post_id show as two separate rows.
    """
    db = get_db()
    await verify_account_ownership(db, account_id, str(current_user["_id"]))

    settings_list = await db["automation_settings"].find(
        {"account_id": account_id, "user_id": str(current_user["_id"])}
    ).sort("updated_at", -1).to_list(length=200)

    result = []
    for s in settings_list:
        automation_id = str(s["_id"])
        post_id       = s["post_id"]

        # Fetch rules linked to this specific automation
        rules = await db["keyword_rules"].find({
            "automation_id": automation_id,
            "user_id":       str(current_user["_id"]),
        }).to_list(length=100)

        # Legacy fallback: rules saved before automation_id existed
        if not rules:
            rules = await db["keyword_rules"].find({
                "post_id":        post_id,
                "account_id":     account_id,
                "user_id":        str(current_user["_id"]),
                "automation_id":  None,
            }).to_list(length=100)

        for r in rules:
            r["id"] = str(r.pop("_id"))

        active_rules = [r for r in rules if r.get("is_active", True)]

        # Count runs by automation_id — NOT post_id (fixes shared count bug)
        runs = await db["automation_logs"].count_documents(
            {"automation_id": automation_id}
        )

        auto_type = []
        if s.get("auto_comment_reply"): auto_type.append("Comment Reply")
        if s.get("auto_dm"):            auto_type.append("DM")
        type_label = " + ".join(auto_type) if auto_type else "None"

        result.append({
            "id":                 automation_id,
            "post_id":            post_id,
            "type":               type_label,
            "rules_count":        len(active_rules),
            "rules":              rules,
            "runs":               runs,
            "is_active":          s.get("is_active", False),
            "auto_comment_reply": s.get("auto_comment_reply", False),
            "auto_dm":            s.get("auto_dm", False),
            "delay_enabled":      s.get("delay_enabled", False),
            # Use created_at for Published — never changes after creation
            "last_published":     s.get("created_at"),
            "created_at":         s.get("created_at"),
        })

    return {"automations": result, "total": len(result)}


@router.get("/detail/{automation_id}")
async def get_automation_detail(
    automation_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Get a single automation with all its rules — used by the Edit page."""
    db = get_db()
    try:
        oid = ObjectId(automation_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid automation ID")

    s = await db["automation_settings"].find_one({
        "_id":     oid,
        "user_id": str(current_user["_id"]),
    })
    if not s:
        raise HTTPException(status_code=404, detail="Automation not found")

    rules = await db["keyword_rules"].find({
        "automation_id": automation_id,
        "user_id":       str(current_user["_id"]),
    }).to_list(length=100)

    if not rules:
        rules = await db["keyword_rules"].find({
            "post_id":       s["post_id"],
            "account_id":    s["account_id"],
            "user_id":       str(current_user["_id"]),
            "automation_id": None,
        }).to_list(length=100)

    for r in rules:
        r["id"] = str(r.pop("_id"))

    return {
        "id":                 str(s["_id"]),
        "post_id":            s["post_id"],
        "account_id":         s["account_id"],
        "auto_comment_reply": s.get("auto_comment_reply", False),
        "auto_dm":            s.get("auto_dm", False),
        "delay_enabled":      s.get("delay_enabled", False),
        "is_active":          s.get("is_active", False),
        "rules":              rules,
        "created_at":         s.get("created_at"),
        "updated_at":         s.get("updated_at"),
    }


@router.put("/detail/{automation_id}")
async def update_automation(
    automation_id: str,
    body: dict,
    current_user: dict = Depends(get_current_user),
):
    """
    Update automation settings + replace all its rules.
    Body: { auto_comment_reply, auto_dm, delay_enabled, is_active, rules: [...] }
    """
    db = get_db()
    try:
        oid = ObjectId(automation_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid automation ID")

    s = await db["automation_settings"].find_one({
        "_id": oid, "user_id": str(current_user["_id"])
    })
    if not s:
        raise HTTPException(status_code=404, detail="Automation not found")

    now = datetime.utcnow()

    # Update settings
    await db["automation_settings"].update_one(
        {"_id": oid},
        {"$set": {
            "auto_comment_reply": body.get("auto_comment_reply", s.get("auto_comment_reply")),
            "auto_dm":            body.get("auto_dm",            s.get("auto_dm")),
            "delay_enabled":      body.get("delay_enabled",      s.get("delay_enabled")),
            "is_active":          body.get("is_active",          s.get("is_active")),
            "updated_at":         now,
        }}
    )

    # Replace rules if provided
    if "rules" in body:
        # Delete old rules for this automation
        await db["keyword_rules"].delete_many({
            "automation_id": automation_id,
            "user_id":       str(current_user["_id"]),
        })
        # Also delete legacy rules
        await db["keyword_rules"].delete_many({
            "post_id":       s["post_id"],
            "account_id":    s["account_id"],
            "user_id":       str(current_user["_id"]),
            "automation_id": None,
        })
        # Insert new rules
        for rule in body["rules"]:
            await db["keyword_rules"].insert_one({
                "user_id":          str(current_user["_id"]),
                "post_id":          s["post_id"],
                "account_id":       s["account_id"],
                "automation_id":    automation_id,
                "trigger_words":    [w.lower().strip() for w in rule.get("trigger_words", [])],
                "response":         rule.get("response", ""),
                "reply_comment":    rule.get("reply_comment", True),
                "send_dm":          rule.get("send_dm", False),
                "is_active":        rule.get("is_active", True),
                "opening_message":  rule.get("opening_message", ""),
                "follow_dm_message":rule.get("follow_dm_message", ""),
                "dm_actions":       rule.get("dm_actions", []),
                "created_at":       now,
                "updated_at":       now,
            })

    return {"status": "updated", "automation_id": automation_id}


@router.delete("/detail/{automation_id}")
async def delete_automation(
    automation_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Delete an automation and all its rules."""
    db = get_db()
    try:
        oid = ObjectId(automation_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid automation ID")

    s = await db["automation_settings"].find_one({
        "_id": oid, "user_id": str(current_user["_id"])
    })
    if not s:
        raise HTTPException(status_code=404, detail="Automation not found")

    deleted_rules = await db["keyword_rules"].delete_many({
        "automation_id": automation_id,
        "user_id":       str(current_user["_id"]),
    })
    # legacy
    await db["keyword_rules"].delete_many({
        "post_id":       s["post_id"],
        "account_id":    s["account_id"],
        "user_id":       str(current_user["_id"]),
        "automation_id": None,
    })

    await db["automation_settings"].delete_one({"_id": oid})
    return {
        "status":        "deleted",
        "automation_id": automation_id,
        "rules_deleted": deleted_rules.deleted_count,
    }


# ═══════════════════════════════════════════════════════════
# LOGS & ANALYTICS
# ═══════════════════════════════════════════════════════════

@router.get("/logs/{post_id}")
async def get_logs(
    post_id: str,
    account_id: str,
    limit: int = 50,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    logs = await db["automation_logs"].find({
        "post_id":    post_id,
        "account_id": account_id,
    }).sort("timestamp", -1).to_list(length=limit)
    for log in logs:
        log["id"] = str(log.pop("_id"))
    return {"logs": logs, "total": len(logs)}


@router.get("/analytics/{post_id}")
async def get_automation_analytics(
    post_id: str,
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    analytics = await db["automation_analytics"].find_one({
        "post_id":    post_id,
        "account_id": account_id,
    })
    if not analytics:
        return {
            "post_id":            post_id,
            "account_id":         account_id,
            "trigger_count":      0,
            "reply_sent_count":   0,
            "dm_sent_count":      0,
            "cooldown_blocked":   0,
            "rate_limit_blocked": 0,
            "last_updated":       None,
        }
    analytics["id"] = str(analytics.pop("_id"))
    return analytics


@router.get("/logs/account/{account_id}")
async def get_account_logs(
    account_id: str,
    limit: int = 100,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    await verify_account_ownership(db, account_id, str(current_user["_id"]))
    logs = await db["automation_logs"].find({
        "account_id": account_id,
    }).sort("timestamp", -1).to_list(length=limit)
    for log in logs:
        log["id"] = str(log.pop("_id"))
    return {"logs": logs, "total": len(logs)}


# ═══════════════════════════════════════════════════════════
# DIAGNOSTICS
# ═══════════════════════════════════════════════════════════

@router.get("/debug/{account_id}")
async def debug_automation(
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    user_id = str(current_user["_id"])
    report  = {}

    try:
        account = await db["instagram_accounts"].find_one(
            {"_id": ObjectId(account_id), "user_id": user_id}
        )
    except Exception:
        return {"error": "Invalid account_id"}
    if not account:
        return {"error": "Account not found"}

    report["account"] = {
        "username":          account.get("username"),
        "instagram_user_id": account.get("instagram_user_id"),
        "is_active":         account.get("is_active"),
        "has_access_token":  bool(account.get("access_token")),
    }

    settings_list = await db["automation_settings"].find(
        {"account_id": account_id, "user_id": user_id}
    ).to_list(length=50)

    report["automation_settings"] = [
        {
            "id":                 str(s["_id"]),
            "post_id":            s["post_id"],
            "is_active":          s.get("is_active"),
            "auto_comment_reply": s.get("auto_comment_reply"),
            "auto_dm":            s.get("auto_dm"),
        }
        for s in settings_list
    ]

    rules_list = await db["keyword_rules"].find(
        {"account_id": account_id, "user_id": user_id}
    ).to_list(length=100)

    report["keyword_rules"] = [
        {
            "post_id":       r["post_id"],
            "automation_id": r.get("automation_id"),
            "trigger_words": r.get("trigger_words"),
            "response":      r.get("response"),
            "is_active":     r.get("is_active"),
        }
        for r in rules_list
    ]

    logs = await db["automation_logs"].find(
        {"account_id": account_id}
    ).sort("timestamp", -1).to_list(length=10)

    report["recent_logs"] = [
        {
            "post_id":      l.get("post_id"),
            "comment_text": l.get("comment_text"),
            "action_taken": l.get("action_taken"),
            "reply_sent":   l.get("reply_sent"),
            "dm_sent":      l.get("dm_sent"),
            "error":        l.get("error"),
            "timestamp":    str(l.get("timestamp")),
        }
        for l in logs
    ]

    issues = []
    if not report["automation_settings"]:
        issues.append("NO automation settings saved")
    if not report["keyword_rules"]:
        issues.append("NO keyword rules saved")
    if not report["recent_logs"]:
        issues.append("NO logs — webhook not firing or not reaching engine")

    report["diagnosis"] = issues or ["No issues found"]
    return report
