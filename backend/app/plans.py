# app/plans.py
"""
Single source of truth for all plan limits and feature gates.
"""

PLAN_LIMITS = {
    "free": {
        "max_accounts":           1,
        "dm_per_day":             10,
        "max_automations":        1,
        "max_scheduled_posts":    3,
        "analytics_days":         7,
        "analytics_export":       False,
        "email_collection":       False,
        "multi_variant_replies":  True,
        "followup_dm":            True,
        "story_reply":            False,
        "leads_view":             True,
        "leads_csv_export":       False,
        "coins_multiplier":       1,
        "priority_support":       False,
    },
    "pro": {
        "max_accounts":           4,
        "dm_per_day":             50,
        "max_automations":        10,
        "max_scheduled_posts":    None,
        "analytics_days":         30,
        "analytics_export":       False,
        "email_collection":       True,
        "multi_variant_replies":  True,
        "followup_dm":            True,
        "story_reply":            True,
        "leads_view":             True,
        "leads_csv_export":       True,
        "coins_multiplier":       2,
        "priority_support":       False,
    },
    "enterprise": {
        "max_accounts":           7,
        "dm_per_day":             100,
        "max_automations":        None,
        "max_scheduled_posts":    None,
        "analytics_days":         365,
        "analytics_export":       True,
        "email_collection":       True,
        "multi_variant_replies":  True,
        "followup_dm":            True,
        "story_reply":            True,
        "leads_view":             True,
        "leads_csv_export":       True,
        "coins_multiplier":       3,
        "priority_support":       True,
    },
}

PLAN_PRICING = {
    "free":       {"price": 0,  "currency": "USD", "interval": None,    "label": "Free"},
    "pro":        {"price": 29, "currency": "USD", "interval": "month", "label": "Pro"},
    "enterprise": {"price": 99, "currency": "USD", "interval": "month", "label": "Enterprise"},
}


def get_plan_limits(plan: str) -> dict:
    return PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])


def check_feature(plan: str, feature: str) -> bool:
    return bool(get_plan_limits(plan).get(feature, False))
