# automation/queue.py
# Redis-backed comment queue with human-like cooldown delays.
#
# Flow:
#   1. Webhook fires → enqueue_comment_event() pushes JSON into
#      Redis list  "comment_queue:{account_id}"
#   2. One background worker loop per account pops jobs one-by-one,
#      calls the engine, then sleeps a human-like delay before the next.
#   3. A single global dispatcher watches a Redis Set
#      "comment_queue:accounts" (accounts with pending jobs)
#      and fans out per-account coroutines automatically.

import asyncio
import json
import logging
import random
from datetime import datetime

from app.redis_pool import get_redis
from app.database import get_db

logger = logging.getLogger(__name__)

# ── Redis keys ────────────────────────────────────────────────────────────────
QUEUE_PREFIX    = "comment_queue:"          # list per account
ACCOUNTS_KEY    = "comment_queue:accounts"  # set of active account_ids
WORKER_LOCK_TTL = 30                        # seconds — per-account worker heartbeat

# ── Human-like inter-comment delay (seconds) ──────────────────────────────────
# Randomised so processing looks organic, not bot-like.
MIN_DELAY = 8
MAX_DELAY = 25


# ── Enqueue ───────────────────────────────────────────────────────────────────

async def enqueue_comment_event(
    account_id: str,
    media_id: str,
    comment_id: str,
    comment_text: str,
    commenter_id: str,
    ig_user_id: str,
    access_token: str,
    commenter_username: str = "",
) -> int:
    """
    Push a comment job onto the per-account Redis queue.
    Returns the new queue depth.
    """
    r = get_redis()
    job = json.dumps({
        "account_id":         account_id,
        "media_id":           media_id,
        "comment_id":         comment_id,
        "comment_text":       comment_text,
        "commenter_id":       commenter_id,
        "ig_user_id":         ig_user_id,
        "access_token":       access_token,
        "commenter_username": commenter_username,
        "enqueued_at":        datetime.utcnow().isoformat(),
    })
    queue_key = f"{QUEUE_PREFIX}{account_id}"
    length = await r.rpush(queue_key, job)      # push to tail (FIFO)
    await r.sadd(ACCOUNTS_KEY, account_id)      # mark account as having work
    logger.info(
        f"[Queue] Enqueued comment_id={comment_id} "
        f"account={account_id} depth={length}"
    )
    return length


# ── Per-account worker ────────────────────────────────────────────────────────

async def _run_account_worker(account_id: str):
    """
    Process comments one-by-one for a single account.
    Applies a human-like random delay between each comment.
    Exits when the queue is empty.
    """
    from app.automation.engine import process_comment_event

    r         = get_redis()
    queue_key = f"{QUEUE_PREFIX}{account_id}"
    lock_key  = f"comment_queue_lock:{account_id}"

    # Soft lock — prevents two workers racing on the same account
    acquired = await r.set(lock_key, "1", nx=True, ex=WORKER_LOCK_TTL)
    if not acquired:
        logger.debug(f"[Queue] Worker already running for account={account_id}, skipping")
        return

    logger.info(f"[Queue] Worker started → account={account_id}")
    try:
        while True:
            # Renew heartbeat so lock doesn't expire mid-batch
            await r.expire(lock_key, WORKER_LOCK_TTL)

            raw = await r.lpop(queue_key)   # pop from head (FIFO)
            if raw is None:
                await r.srem(ACCOUNTS_KEY, account_id)
                logger.info(f"[Queue] Queue drained → account={account_id}, worker exiting")
                break

            try:
                job = json.loads(raw)
            except Exception as e:
                logger.error(f"[Queue] Bad JSON job for account={account_id}: {e}")
                continue

            logger.info(
                f"[Queue] Processing → comment_id={job.get('comment_id')} "
                f"commenter={job.get('commenter_id')} "
                f"text='{job.get('comment_text')}'"
            )

            try:
                db = get_db()
                await process_comment_event(
                    db=db,
                    media_id=job["media_id"],
                    comment_id=job["comment_id"],
                    comment_text=job["comment_text"],
                    commenter_id=job["commenter_id"],
                    account_id=job["account_id"],
                    ig_user_id=job["ig_user_id"],
                    access_token=job["access_token"],
                    commenter_username=job.get("commenter_username", ""),
                )
            except Exception as e:
                logger.error(
                    f"[Queue] Engine error for comment_id={job.get('comment_id')}: {e}",
                    exc_info=True,
                )

            # ── Human-like delay before picking up next comment ───────────────
            remaining = await r.llen(queue_key)
            if remaining > 0:
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                logger.info(
                    f"[Queue] {remaining} comment(s) still queued for account={account_id}. "
                    f"Sleeping {delay:.1f}s (human-like cooldown)…"
                )
                await asyncio.sleep(delay)

    finally:
        await r.delete(lock_key)


# ── Global dispatcher ─────────────────────────────────────────────────────────

_worker_tasks: dict = {}   # account_id → asyncio.Task


async def _dispatch_workers():
    """
    Poll the active-accounts Redis Set every 3 seconds.
    For any account that has queued work but no live task, spawn one.
    """
    r = get_redis()
    while True:
        try:
            active_accounts = await r.smembers(ACCOUNTS_KEY)
            for account_id in active_accounts:
                if isinstance(account_id, bytes):
                    account_id = account_id.decode()
                existing = _worker_tasks.get(account_id)
                # Bug #13 fix: prune completed tasks before spawning a replacement.
                # Without this the dict grows forever (one entry per account, never removed),
                # causing a slow memory leak on long-running servers.
                if existing is not None and existing.done():
                    del _worker_tasks[account_id]
                    existing = None
                if existing is not None:
                    continue
                task = asyncio.create_task(
                    _run_account_worker(account_id),
                    name=f"queue_worker_{account_id}",
                )
                _worker_tasks[account_id] = task
                logger.info(f"[Queue] Dispatcher spawned worker → account={account_id}")
        except Exception as e:
            logger.error(f"[Queue] Dispatcher error: {e}", exc_info=True)

        await asyncio.sleep(3)


_dispatcher_task = None


def start_queue_worker():
    """Call once at app startup (inside lifespan)."""
    global _dispatcher_task
    if _dispatcher_task and not _dispatcher_task.done():
        logger.warning("[Queue] Dispatcher already running")
        return
    _dispatcher_task = asyncio.create_task(
        _dispatch_workers(),
        name="comment_queue_dispatcher",
    )
    logger.info("[Queue] Comment queue dispatcher started ✅")


def stop_queue_worker():
    """Call once at app shutdown."""
    global _dispatcher_task
    if _dispatcher_task:
        _dispatcher_task.cancel()
        _dispatcher_task = None
        logger.info("[Queue] Comment queue dispatcher stopped")
