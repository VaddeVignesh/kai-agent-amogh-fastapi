# fix_sqlguard.py
from pathlib import Path
import shutil

GUARD = Path(r"C:\Users\vgangadhar\kai-agent-amogh-fastapi\app\sql\sql_guard.py")
text = GUARD.read_text(encoding="utf-8")
shutil.copy(GUARD, str(GUARD) + ".bak")

OLD = (
    "                col_name = parts[-1].split(\".\")[-1].lower()\n"
    "\n"
    "                if col_name in _INVALID_COLUMNS:"
)

NEW = (
    "                # Check source column, not alias\n"
    "                # e.g. 'port_text AS port_name' → check 'port_text', not 'port_name'\n"
    "                if len(parts) >= 3 and parts[-2].lower() == \"as\":\n"
    "                    col_name = parts[0].split(\".\")[-1].lower()\n"
    "                else:\n"
    "                    col_name = parts[-1].split(\".\")[-1].lower()\n"
    "\n"
    "                if col_name in _INVALID_COLUMNS:"
)

if OLD in text:
    text = text.replace(OLD, NEW, 1)
    print("✅ Fixed: guard now checks source column not alias")
    print("   port_text AS port_name  → checks 'port_text'  ✓ allowed")
    print("   grade_text AS cargo_grade → checks 'grade_text' ✓ allowed")
    print("   vessel_id AS x          → checks 'vessel_id'  ✗ blocked")
else:
    print("❌ Pattern not found")

GUARD.write_text(text, encoding="utf-8")