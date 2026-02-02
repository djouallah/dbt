import sqlite3
conn = sqlite3.connect(r'c:\lakehouse\default\Files\dbt\metadata.db')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("Tables:", cur.fetchall())
cur.execute("SELECT name FROM sqlite_master WHERE name LIKE '%csv%' OR name LIKE '%archive%'")
print("csv/archive tables:", cur.fetchall())
