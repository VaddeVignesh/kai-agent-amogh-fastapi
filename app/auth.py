import uuid

ROLE_ACCESS = {
    "customer": {
        "postgres_tables": ["finance_voyage_kpi", "ops_voyage_summary"],
        "mongo_collections": ["vessels", "voyages"],
        "redis": [],
        "admin_apis": [],
    },
    "admin": {
        "postgres_tables": ["finance_voyage_kpi", "ops_voyage_summary"],
        "mongo_collections": ["vessels", "voyages"],
        "redis": ["admin_metrics", "admin_users", "audit_log", "system_health"],
        "admin_apis": ["/admin/metrics", "/admin/users", "/admin/audit-log", "/admin/system-health"],
    },
}

USERS = {
    # Backward-compatible demo users used by existing local flows.
    "admin": {
        "password": "admin123",
        "role": "admin",
    },
    "customer": {
        "password": "cust123",
        "role": "customer",
    },
    # Multi-login demo users for validating session isolation across accounts.
    "admin1": {"password": "admin123", "role": "admin"},
    "admin2": {"password": "admin223", "role": "admin"},
    "admin3": {"password": "admin323", "role": "admin"},
    "admin4": {"password": "admin423", "role": "admin"},
    "admin5": {"password": "admin523", "role": "admin"},
    "customer1": {"password": "cust123", "role": "customer"},
    "customer2": {"password": "cust223", "role": "customer"},
    "customer3": {"password": "cust323", "role": "customer"},
    "customer4": {"password": "cust423", "role": "customer"},
    "customer5": {"password": "cust523", "role": "customer"},
}

def login(username: str, password: str):
    user = USERS.get(username)
    if not user:
        return None
    if user["password"] != password:
        return None
    return user["role"]

def get_role_access(role: str) -> dict:
    return dict(ROLE_ACCESS.get(role, {}))

def generate_session_id(username: str, role: str) -> str:
    unique = uuid.uuid4().hex[:12]
    return f"{role}:{username}:{unique}"