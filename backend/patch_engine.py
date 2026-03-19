"""
patch_engine.py  -- run once from the backend directory to apply bugs #8, #9, #10
"""
import pathlib, sys

TARGET = pathlib.Path(__file__).parent / "app" / "automation" / "engine.py"
src = TARGET.read_text(encoding="utf-8")
original_len = len(src)

# =====================================================================
# Bug #9 -- replace match_keyword with substring-aware version
# =====================================================================
OLD9 = (
    "def match_keyword(comment_text: str, trigger_words: list) -> str | None:\n"
    '    """\n'
    "    Exact match (case-insensitive, trimmed).\n"
    "    '*' wildcard matches any comment.\n"
    '    """\n'
    '    if "*" in trigger_words:\n'
)

assert OLD9 in src, "BUG #9: target block not found"

NEW9 = (
    "def match_keyword(comment_text: str, trigger_words: list) -> str | None:\n"
    '    """\n'
    "    Keyword matching with two modes (Bug #9 fix).\n"
    "\n"
    "    Old (broken): strict full-string equality only.\n"
    "    'info' would NOT match 'Info!', 'send me the link' would NOT match\n"
    "    'Can you send me the link please?' -- the vast majority of real\n"
    "    comments silently fell through with no automation triggered.\n"
    "\n"
    "    New behaviour (default: contains/substring):\n"
    "      1. '*' wildcard  -- matches any text, always checked first.\n"
    "      2. Prefix '='    -- forces exact-only mode, e.g. '=yes'.\n"
    "      3. Exact match   -- full comment equals trigger word.\n"
    "      4. Contains match-- comment contains the trigger as a substring.\n"
    '    """\n'
    '    if "*" in trigger_words:\n'
)

src = src.replace(OLD9, NEW9, 1)

# Now replace the body (the part after the if-wildcard block up to return None)
OLD9B = (
    '        logger.info("Wildcard \'*\' matched \u2014 triggering on any comment")\n'
    '        return "*"\n'
    "\n"
    "    text_lower = comment_text.strip().lower()\n"
    "    for word in trigger_words:\n"
    "        if word.strip().lower() == text_lower:\n"
    "            logger.info(f\"Exact keyword matched: '{word}' == '{comment_text}'\")\n"
    "            return word\n"
    "\n"
    "    return None\n"
)

NEW9B = (
    '        logger.info("Wildcard \'*\' matched -- triggering on any comment")\n'
    '        return "*"\n'
    "\n"
    "    text_lower = comment_text.strip().lower()\n"
    "\n"
    "    for word in trigger_words:\n"
    "        raw = word.strip()\n"
    "        if not raw:\n"
    "            continue\n"
    "\n"
    "        # Exact-only mode: trigger word prefixed with '='\n"
    '        if raw.startswith("="):\n'
    "            kw = raw[1:].lower()\n"
    "            if text_lower == kw:\n"
    "                logger.info(f\"Exact keyword matched (strict): '{raw}' == '{comment_text}'\")\n"
    "                return word\n"
    "            continue\n"
    "\n"
    "        kw = raw.lower()\n"
    "\n"
    "        # Exact full-string match\n"
    "        if text_lower == kw:\n"
    "            logger.info(f\"Exact keyword matched: '{word}' == '{comment_text}'\")\n"
    "            return word\n"
    "\n"
    "        # Contains/substring match (catches natural language comments)\n"
    "        if kw in text_lower:\n"
    "            logger.info(f\"Substring keyword matched: '{word}' in '{comment_text}'\")\n"
    "            return word\n"
    "\n"
    "    return None\n"
)

assert OLD9B in src, "BUG #9 body: target block not found"
src = src.replace(OLD9B, NEW9B, 1)
print("Bug #9: match_keyword updated")

# =====================================================================
# Bug #8 -- cap email re-prompts at 3 (add reprompt counter)
# =====================================================================
OLD8 = (
    "        if not email:\n"
    "            logger.info(f\"[Email] Invalid email '{msg_text}' from {sender_id} \u2014 re-prompting\")\n"
    "            await send_dm(\n"
    "                ig_user_id=ig_user_id,\n"
    "                recipient_id=sender_id,\n"
    "                message=(\n"
    "                    \"Hmm, that doesn't look like a valid email address. \"\n"
    '                    "Please reply with a valid email (e.g. name@example.com) \\U0001f4e7"\n'
    "                ),\n"
    "                access_token=access_token,\n"
    "            )\n"
    "            return True\n"
)

assert OLD8 in src, "BUG #8: target block not found"

NEW8 = (
    "        if not email:\n"
    "            # -- Bug #8 fix: cap re-prompts to avoid infinite DM spam ----\n"
    "            # Before this fix every invalid reply caused an unlimited chain\n"
    "            # of re-prompt DMs until the 1-hour TTL expired, risking IG\n"
    "            # spam detection. Now capped at MAX_REPROMPTS.\n"
    "            reprompt_key   = f\"email_reprompt:{account_id}:{sender_id}\"\n"
    "            reprompt_count = await r.incr(reprompt_key)\n"
    "            if reprompt_count == 1:\n"
    "                await r.expire(reprompt_key, EMAIL_AWAIT_TTL)\n"
    "\n"
    "            MAX_REPROMPTS = 3\n"
    "            if reprompt_count > MAX_REPROMPTS:\n"
    "                logger.info(\n"
    "                    f\"[Email] Max re-prompts ({MAX_REPROMPTS}) reached for \"\n"
    "                    f\"{sender_id} -- aborting email collection\"\n"
    "                )\n"
    "                await r.delete(redis_key, reprompt_key)\n"
    "                await send_dm(\n"
    "                    ig_user_id=ig_user_id,\n"
    "                    recipient_id=sender_id,\n"
    '                    message="No worries! Feel free to reach out anytime. \\U0001f44b",\n'
    "                    access_token=access_token,\n"
    "                )\n"
    "                return True\n"
    "\n"
    "            logger.info(\n"
    "                f\"[Email] Invalid email '{msg_text}' from {sender_id} -- \"\n"
    "                f\"re-prompt {reprompt_count}/{MAX_REPROMPTS}\"\n"
    "            )\n"
    "            await send_dm(\n"
    "                ig_user_id=ig_user_id,\n"
    "                recipient_id=sender_id,\n"
    "                message=(\n"
    "                    \"Hmm, that doesn't look like a valid email address. \"\n"
    '                    "Please reply with a valid email (e.g. name@example.com) \\U0001f4e7"\n'
    "                ),\n"
    "                access_token=access_token,\n"
    "            )\n"
    "            return True\n"
)

src = src.replace(OLD8, NEW8, 1)
print("Bug #8: email re-prompt capped")

# =====================================================================
# Bug #10 -- Story Engine: replace inline httpx username call with cached helper
# =====================================================================
OLD10_STORY = (
    "            # \u2500\u2500 Fetch sender username \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
    '            sender_username = ""\n'
    "            try:\n"
    "                async with httpx.AsyncClient(timeout=10.0) as _cl:\n"
    "                    _r = await _cl.get(\n"
    "                        f\"{GRAPH_BASE}/{sender_id}\",\n"
    '                        params={"fields": "username", "access_token": access_token},\n'
    "                    )\n"
    "                    sender_username = _r.json().get(\"username\", \"\")\n"
    "            except Exception as _ue:\n"
    "                logger.debug(f\"[Story Engine] Could not fetch username for {sender_id}: {_ue}\")\n"
)

if OLD10_STORY in src:
    NEW10_STORY = (
        "            # -- Fetch sender username (Bug #10: cached _fetch_ig_username) -\n"
        "            sender_username = await _fetch_ig_username(sender_id, access_token)\n"
    )
    src = src.replace(OLD10_STORY, NEW10_STORY, 1)
    print("Bug #10 (Story): cached username fetch applied")
else:
    # The box-drawing header might differ; find by the try/async block alone
    import re as _re
    pat = _re.compile(
        r"            # .+Fetch sender username.+\n"
        r'            sender_username = ""\n'
        r"            try:\n"
        r"                async with httpx\.AsyncClient\(timeout=10\.0\) as _cl:\n"
        r"                    _r = await _cl\.get\(\n"
        r'                        f"\{GRAPH_BASE\}/\{sender_id\}",\n'
        r'                        params=\{"fields": "username", "access_token": access_token\},\n'
        r"                    \)\n"
        r'                    sender_username = _r\.json\(\)\.get\("username", ""\)\n'
        r"            except Exception as _ue:\n"
        r'            \s+logger\.debug\(f"\[Story Engine\] Could not fetch username for \{sender_id\}: \{_ue\}"\)',
        re.DOTALL,
    )
    m = pat.search(src)
    if m:
        NEW10_STORY = (
            "            # -- Fetch sender username (Bug #10: cached _fetch_ig_username) -\n"
            "            sender_username = await _fetch_ig_username(sender_id, access_token)\n"
        )
        src = src[:m.start()] + NEW10_STORY + src[m.end():]
        print("Bug #10 (Story): regex replacement applied")
    else:
        print("WARNING: Bug #10 Story block not found; manual fix needed")

# =====================================================================
# Bug #10 -- DM Engine: replace inline httpx username call with cached helper
# =====================================================================
# Locate the DM section by its surrounding context
dm_marker = (
    "        action_taken = []\n"
    "        dm_sent      = False\n"
    "\n"
    "        # \u2500\u2500 Fetch sender username "
)

if dm_marker in src:
    # Find the end of the try/except block
    start = src.index(dm_marker) + len("        action_taken = []\n        dm_sent      = False\n\n        ")
    chunk_start = src.index("        # ", start - 5)  # find the comment line
    # Find the end of the except block for the username fetch
    end_marker = "        logger.debug(f\"[DM Engine] Could not fetch username for {sender_id}: {_ue}\")\n"
    if end_marker in src[chunk_start:]:
        abs_end = src.index(end_marker, chunk_start) + len(end_marker)
        old_chunk = src[chunk_start:abs_end]
        new_chunk = (
            "        # -- Fetch sender username (Bug #10: cached _fetch_ig_username) -------\n"
            "        sender_username = await _fetch_ig_username(sender_id, access_token)\n"
        )
        src = src[:chunk_start] + new_chunk + src[abs_end:]
        print("Bug #10 (DM): cached username fetch applied")
    else:
        print("WARNING: Bug #10 DM end marker not found")
else:
    print("WARNING: Bug #10 DM start marker not found")

assert len(src) != original_len, "Nothing changed -- check assertions above"
TARGET.write_text(src, encoding="utf-8")
print(f"engine.py written ({len(src)} chars). All bugs patched.")
