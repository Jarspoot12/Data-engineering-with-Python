# Exercise: Normalize + Deduplicate + Aggregate “Event Logs”
""" You receive messy event logs from multiple sources (API + mobile). Each event is a dictionary, but:

- keys are inconsistent ("UserID", "user_id", "uid")
- values have mixed types ("101" vs 101, "true" vs True)
- some records are duplicated
- some records are missing fields
- nested data exists (payload dict)
- you must produce clean outputs without mutating the original input (aliasing trap)"""


## Input data
raw_events = [
    {"EventID": "e1", "UserID": "101", "action": "LOGIN", "active": "true", "payload": {"ip": "10.0.0.1", "device": "Android"}},
    {"event_id": "e1", "user_id": 101, "action": "LOGIN", "active": True, "payload": {"ip": "10.0.0.1", "device": "Android"}},  # duplicate of e1
    {"event_id": "e2", "uid": "102", "action": "purchase", "active": "False", "payload": {"ip": "10.0.0.2", "items": ["A12", "A12", "B07"]}},
    {"event_id": "e3", "user_id": "101", "action": "Purchase", "active": "true", "payload": {"ip": "10.0.0.1", "items": ["B07"]}},
    {"event_id": "e4", "user_id": None, "action": "login", "active": "true", "payload": {"ip": "10.0.0.9"}},
    {"event_id": "e5", "user_id": "103", "action": "logout", "active": "true", "payload": {}},
    {"event_id": "e6", "user_id": "102", "action": "purchase", "active": "true", "payload": {"ip": "10.0.0.2", "items": []}},
]

## Step 1. Build small helper functions for different data types
def to_int_or_none(value):
    if value == None:
        return None
    elif isinstance(value, int):
        return value
    elif isinstance(value, str):
        value = value.strip()
        try:
            return int(value)
        except ValueError:
            return None
    else:
        return None
    
def to_bool(value):
    if isinstance(value, bool):
        return value
    elif isinstance(value, int):  # 1 -> True, everything else -> False
        value = True if value == 1 else False
        return value
    elif isinstance(value, str):
        value = value.strip().lower()
        truthy = {"true", "1", "yes", "y", "t"}
        value = True if value in truthy else False
        return value
    else:
        return False

def dedupe_preserve_order(items):
    seen = set()
    result = []
    for x in items:
        if x not in seen:
            result.append(x)
            seen.add(x)
    return result

## Step 2 — Build ONE normalized event first, then iterate through the events
""" Target schema
{
  "event_id": str,
  "user_id": int | None,
  "action": str,
  "active": bool,
  "ip": str | None,
  "items": list[str]
}
"""
def normalize_event(e):
    ##### extract values
    payload = e.get("payload", {}) or {}
    ### event_id
    raw_event_id = e.get("event_id") or e.get("EventID")
    event_id = str(raw_event_id) if raw_event_id is not None else None
    ### user_id
    raw_user = e.get("user_id")
    if raw_user is None:
        raw_user = e.get("UserID")
    if raw_user is None:
        raw_user = e.get("uid")
    user_id = to_int_or_none(raw_user)
    ### action
    action = str(e.get("action", "")).strip().lower()
    ### active
    active = to_bool(e.get("active", False))
    ### ip
    ip = payload.get("ip") or None #If the value is falsy, return None
    ### items
    items = payload.get("items", []) or []
    items = dedupe_preserve_order(list(items))
        
    return {
        "event_id": event_id,
        "user_id": user_id,
        "action": action,
        "active": active,
        "ip": ip,
        "items": items,
    }



def normalize_events(events):
    normalized = []
    for e in events:
        normalized.append(normalize_event(e))
    return normalized

# print(normalize_events(raw_events))

# clean = normalize_events(raw_events)
# print(clean[0])
# print(clean[2])

## Step 3. — Deduplicate events (composite key + keep first)
"""Two events are duplicates if they share the same composite key:
(event_id, user_id, action, ip). We will keep the first occurrence."""

def deduplicate_events(clean_events):
    seen_keys = set()
    deduped = []
    for e in clean_events:
        key = (
            e["event_id"],
            e["user_id"],
            e["action"],
            e["ip"]
        )
        if key not in seen_keys:
            deduped.append(e)
            seen_keys.add(key)
    return deduped

clean = normalize_events(raw_events)
deduped = deduplicate_events(clean)

# print("raw:", len(raw_events))
# print("clean:", len(clean))
# print("deduped:", len(deduped))

# # Optional: show event_ids so you can visually confirm e1 duplicate was removed
# print([e["event_id"] for e in deduped])

## Step 4: build the summary aggregations.
"""We will get:
{
  "events_per_user": {user_id: count},                 
  "unique_ips_per_user": {user_id: set_of_ips},        
  "purchase_items_per_user": {user_id: set_of_items},  
  "active_users": set_of_user_ids                      
}
Rules:
- Ignore user_id is None for the per-user dicts and active_users
- Ignore ip is None for unique ips
- Only include items for events where action == "purchase"
"""

# initialize the summary containers
def build_summary(clean_events):
    summary = {
        "events_per_user": {},
        "unique_ips_per_user": {},
        "purchase_items_per_user": {},
        "active_users": set(),
    }
    
    # fill summary
    for e in clean_events:
        # events_per_user
        user_id = e.get("user_id", None)
        if user_id is not None:
            summary["events_per_user"][user_id] = summary["events_per_user"].get(user_id, 0) + 1
        
        # unique_ips_per_user
        ip = e.get("ip") or None
        if user_id is not None and ip is not None:
            summary["unique_ips_per_user"].setdefault(user_id, set()).add(ip)
        
        # purchase_items_per_user
        if user_id is not None and e.get("action") == "purchase":
            summary["purchase_items_per_user"].setdefault(user_id, set()).update(e["items"]) # we use update() to add many iterables

        # active users
        if user_id is not None and e.get("active") is True:
            summary["active_users"].add(user_id)

    return summary

# Run it on deduped events
clean = normalize_events(raw_events)
deduped = deduplicate_events(clean)

summary = build_summary(deduped)


print("events per user:", summary["events_per_user"])
print("unique ips per user:", summary["unique_ips_per_user"])
print("purchase items per user:", summary["purchase_items_per_user"])
print("active users:", summary["active_users"])

"""
This exercise covered core Python fundamentals for data engineering:
- Normalizing heterogeneous data types (strings, integers, booleans)
- Safely handling missing and malformed values
- Working in depth with Python collections (lists, sets, dictionaries)
- Removing duplicates using composite keys and sets
- Aggregating data using dictionary-based patterns
- Avoiding aliasing bugs by rebuilding data structures instead of copying them

Key takeaway:
Rebuild nested structures explicitly whenever possible.
Use deepcopy() only when nested mutable objects are reused and must remain independent.

These patterns form the foundation of reliable ETL and data processing pipelines in Python.
"""