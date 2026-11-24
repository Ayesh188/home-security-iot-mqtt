import sqlite3
from datetime import datetime

db = sqlite3.connect("home_security.db")
rows = db.execute(
    "SELECT timestamp FROM readings ORDER BY id DESC LIMIT 600"
).fetchall()

times = sorted(datetime.fromisoformat(r[0]) for r in rows if r[0])
print("Readings checked:", len(times))
print(f"From {times[0]:%H:%M:%S} to {times[-1]:%H:%M:%S}")

found = False
for a, b in zip(times, times[1:]):
    gap = (b - a).total_seconds()
    if gap > 3:
        found = True
        print(f"Gap of {gap:.1f} s between {a:%H:%M:%S} and {b:%H:%M:%S}")

if not found:
    print("No gaps above 3 s: no readings were missing.")