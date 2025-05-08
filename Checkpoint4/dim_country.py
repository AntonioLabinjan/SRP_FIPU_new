import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert
from sqlalchemy.exc import SQLAlchemyError

# -------------------------------
# 🔧 KONFIGURACIJA – prilagodi ovo
CSV_PATH = "C:/Users/Korisnik/Desktop/SRP_ETL/ElectionData.csv"
CSV_COLUMN = 'territoryName'

TABLE_NAME = 'dim_country'
TABLE_COLUMN = 'name'

# Zamijeni sa svojim MySQL pristupom
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_NAME = 'DIMENSIONAL_DATABASE'

# -------------------------------

# MySQL connection string (kroz mysql+mysqlconnector)
DATABASE_URL = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'

# Poveži se s bazom
engine = create_engine(DATABASE_URL)
conn = engine.connect()

# Inicijaliziraj metadata bez 'bind' argumenta
metadata = MetaData()
metadata.reflect(bind=engine)

# Dohvati tablicu
if TABLE_NAME not in metadata.tables:
    raise ValueError(f"Tablica '{TABLE_NAME}' ne postoji u bazi!")

table = metadata.tables[TABLE_NAME]

# Ispis svih stupaca
print(table.columns.keys())  # Provjeri naziv stupca

# Učitaj CSV
df = pd.read_csv(CSV_PATH)

# Ukloni duplikate ako treba
unique_values = df[CSV_COLUMN].dropna().drop_duplicates()

# Dohvati trenutni najveći ID (zamijeni 'id' sa stvarnim nazivom stupca)
max_id_result = conn.execute(table.select().with_only_columns(table.c.country_id).order_by(table.c.country_id.desc()).limit(1)).fetchone()
max_id = max_id_result[0] if max_id_result else 0

# Unesi podatke
for idx, value in enumerate(unique_values, start=max_id + 1):
    try:
        # Provjeri postoji li već (opcionalno)
        exists = conn.execute(table.select().where(table.c[TABLE_COLUMN] == value)).first()
        if exists:
            print(f"Preskacem (vec postoji): {value}")
            continue

        # Unesi podatke s novim ID-em
        ins = insert(table).values({table.c.country_id: idx, TABLE_COLUMN: value})  # Zamijeni 'country_id' s pravim nazivom ID stupca
        conn.execute(ins)
        print(f" Ubacio (ID: {idx}): {value}")
        
        # Commit za svaki unos (ako želite)
        conn.commit()
    except SQLAlchemyError as e:
        print(f" Greška pri unosu '{value}': {e}")

result = conn.execute(table.select()).fetchall()

# Ispis svih podataka
print("\nPodaci u tablici nakon unosa:")
for row in result:
    print(row)
    
# Zatvori konekciju nakon unosa
conn.close()
