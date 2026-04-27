import uuid

USERS = {
    "admin": {
        "password": "admin123",
        "role": "admin",
    },
    "customer": {
        "password": "cust123",
        "role": "customer",
    },
}

def login(username: str, password: str):
    user = USERS.get(username)
    if not user:
        return None
    if user["password"] != password:
        return None
    return user["role"]

def generate_session_id(username: str, role: str) -> str:
    unique = uuid.uuid4().hex[:12]
    return f"{role}:{username}:{unique}"