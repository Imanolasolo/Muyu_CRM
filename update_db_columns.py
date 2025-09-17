import sqlite3

DB_PATH = "muyu_crm.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Intenta agregar las columnas, ignora el error si ya existen
try:
    c.execute("ALTER TABLE institutions ADD COLUMN pais TEXT;")
except sqlite3.OperationalError:
    pass
try:
    c.execute("ALTER TABLE institutions ADD COLUMN ciudad TEXT;")
except sqlite3.OperationalError:
    pass
try:
    c.execute("ALTER TABLE institutions ADD COLUMN direccion TEXT;")
except sqlite3.OperationalError:
    pass

conn.commit()
conn.close()
print("Columnas pais, ciudad y direccion agregadas (o ya existen).")
