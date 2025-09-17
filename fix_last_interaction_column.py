import sqlite3

DB_PATH = "muyu_crm.db"
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Cambiar el tipo de columna last_interaction a TEXT
# SQLite no permite ALTER COLUMN directamente, así que hay que crear una nueva tabla temporal
c.execute("PRAGMA foreign_keys=off;")

# Renombrar la tabla original
c.execute("ALTER TABLE institutions RENAME TO institutions_old;")

# Crear la nueva tabla con last_interaction como TEXT
c.execute('''
CREATE TABLE institutions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    rector TEXT,
    rector_position TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    website TEXT,
    pais TEXT,
    ciudad TEXT,
    direccion TEXT,
    created_contact TEXT,
    last_interaction TEXT,
    num_teachers INTEGER,
    num_students INTEGER,
    avg_fee REAL,
    initial_contact_medium TEXT,
    stage TEXT,
    substage TEXT,
    program_proposed TEXT,
    proposal_value REAL,
    observations TEXT,
    assigned_commercial TEXT,
    no_interest_reason TEXT
);
''')

# Copiar los datos
c.execute('''
INSERT INTO institutions SELECT * FROM institutions_old;
''')

# Eliminar la tabla antigua
c.execute("DROP TABLE institutions_old;")

c.execute("PRAGMA foreign_keys=on;")
conn.commit()
conn.close()
print("Columna last_interaction cambiada a TEXT.")
