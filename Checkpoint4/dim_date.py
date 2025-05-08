import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# -------------------------------
# KONFIGURACIJA – prilagodi ovo
CSV_PATH = "C:/Users/Korisnik/Desktop/SRP_ETL/ElectionData.csv"
CSV_COLUMN = 'time'  # assuming the CSV has time in HH:MM:SS format

TABLE_NAME = 'dim_date'
TABLE_COLUMN = 'full_date'

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_NAME = 'DIMENSIONAL_DATABASE'
# -------------------------------

# MySQL connection string (preko mysqlconnector)
DATABASE_URL = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(DATABASE_URL)
conn = engine.connect()

# Inicijalizacija metapodataka
metadata = MetaData()
metadata.reflect(bind=engine)

# Provjeri postoji li tablica
if TABLE_NAME not in metadata.tables:
    raise ValueError(f"Tablica '{TABLE_NAME}' ne postoji u bazi!")

table = metadata.tables[TABLE_NAME]
print(table.columns.keys())  # Provjera stupaca

# Učitaj CSV
df = pd.read_csv(CSV_PATH)

# Ukloni duplikate i prazne vrijednosti
unique_values = df[CSV_COLUMN].dropna().drop_duplicates()

# Dohvati trenutni najveći date_tk
max_id_result = conn.execute(table.select()
                             .with_only_columns(table.c.date_tk)
                             .order_by(table.c.date_tk.desc())
                             .limit(1)).fetchone()
max_id = max_id_result[0] if max_id_result else 0

# Inicijalizacija start_time i end_time
start_time = datetime.strptime("18:00:00", "%H:%M:%S")
end_time = datetime.strptime("23:00:00", "%H:%M:%S")

# Petlja po jedinstvenim datumima
for idx, value in enumerate(unique_values, start=max_id + 1):
    try:
        # Pretvori string u datetime.datetime za full_date
        full_date = pd.to_datetime(value).to_pydatetime()

        year = full_date.year
        month = full_date.month
        day = full_date.day
        weekday = full_date.strftime('%A')

        # Generiraj timestamp s vremenskim intervalima
        timestamp_start = datetime.combine(full_date, start_time.time())  # Combine date and start time
        timestamp_end = datetime.combine(full_date, end_time.time())      # Combine date and end time

        # Insert u dim_date sa start_time i end_time
        ins = insert(table).values({
            'date_tk': idx,
            'full_date': full_date,
            'year': year,
            'month': month,
            'day': day,
            'weekday': weekday,
            'timestamp': timestamp_start  # You can also insert end_time if needed
        })
        conn.execute(ins)
        print(f" Ubacio (ID: {idx}): {full_date} ({weekday}) sa timestamp {timestamp_start}.")

        conn.commit()

    except Exception as e:
        print(f" Greska za '{value}': {e}")

# Ispis svih podataka
result = conn.execute(table.select()).fetchall()
print(" Podaci u tablici nakon unosa:")
for row in result:
    print(row)

# Zatvori konekciju
conn.close()
