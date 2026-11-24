import sqlite3
import pandas as pd

db = sqlite3.connect("home_security.db")
df = pd.read_sql("SELECT * FROM readings", db)

print("Total readings:", len(df))
print(df.tail())
print()
print("Average per room and sensor:")
print(df.groupby(["room", "sensor"])["value"].mean().round(1))
print()
print("Maximum per sensor:")
print(df.groupby("sensor")["value"].max())