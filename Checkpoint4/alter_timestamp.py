import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, update
from datetime import datetime, timedelta

# KONFIGURACIJA
TABLE_NAME = 'dim_date'
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_NAME = 'DIMENSIONAL_DATABASE'

# MySQL connection string
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

# Dohvati sve jedinstvene full_date iz dim_date
full_dates = conn.execute(table.select().with_only_columns(table.c.full_date).distinct()).fetchall()

# Petlja za svaki jedinstveni full_date
for full_date_row in full_dates:
    full_date = full_date_row[0]
    
    # Početni i završni vremenski interval
    start_time = datetime.strptime("18:00:00", "%H:%M:%S")
    end_time = datetime.strptime("22:00:00", "%H:%M:%S")
    
    # Inkrementiranje kroz sate od 18:00 do 22:00
    current_time = start_time
    while current_time <= end_time:
        # Kombiniraj datum i vrijeme
        timestamp = datetime.combine(full_date, current_time.time())

        # Provjeri postoji li već timestamp za ovaj full_date i timestamp
        existing_timestamp = conn.execute(
            table.select().where(table.c.full_date == full_date).where(table.c.timestamp == timestamp)
        ).fetchone()
        
        if not existing_timestamp:
            # Ako timestamp ne postoji, ažuriraj unos
            stmt = update(table).where(table.c.full_date == full_date).values({
                'timestamp': timestamp
            })
            conn.execute(stmt)
            print(f"Ažuriran timestamp: {timestamp}")
        
        # Inkrementiraj vrijeme za jedan sat
        current_time += timedelta(hours=1)

# Zatvori konekciju
conn.close()
