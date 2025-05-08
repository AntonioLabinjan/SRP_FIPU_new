import random
from sqlalchemy import create_engine, MetaData, Table, update

# -------------------------------
# 🔧 KONFIGURACIJA – prilagodi ovo
TABLE_NAME = 'dim_country'
TABLE_COLUMN = 'region'

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_NAME = 'DIMENSIONAL_DATABASE'
# -------------------------------

DATABASE_URL = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'

engine = create_engine(DATABASE_URL)
conn = engine.connect()
metadata = MetaData()
metadata.reflect(bind=engine)

if TABLE_NAME not in metadata.tables:
    raise ValueError(f"Tablica '{TABLE_NAME}' ne postoji u bazi!")

table = metadata.tables[TABLE_NAME]
regions = ['east', 'west', 'south', 'north', 'center']

# Startamo transakciju
with engine.begin() as transaction_conn:
    results = transaction_conn.execute(table.select().where(table.c.region.is_(None))).fetchall()

    for row in results:
        random_region = random.choice(regions)
        try:
            stmt = (
                update(table)
                .where(table.c.country_id == row.country_id)
                .values({table.c.region: random_region})
            )
            transaction_conn.execute(stmt)
            print(f"Ažuriran zapis ID: {row.country_id} s regionom: {random_region}")
        except Exception as e:
            print(f"Greška pri ažuriranju ID: {row.country_id} - {e}")
