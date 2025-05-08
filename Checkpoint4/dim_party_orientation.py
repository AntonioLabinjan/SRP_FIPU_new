import random
from sqlalchemy import create_engine, MetaData, Table, update

# -------------------------------
# 🔧 KONFIGURACIJA – prilagodi ovo
TABLE_NAME = 'dim_party'
TABLE_COLUMN = 'orientation'

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
orientations = ['Center', 'Left', 'Right']

# Startamo transakciju
with engine.begin() as transaction_conn:
    results = transaction_conn.execute(table.select().where(table.c.orientation.is_(None))).fetchall()

    for row in results:
        random_orientation = random.choice(orientations)
        try:
            stmt = (
                update(table)
                .where(table.c.party_id == row.party_id)
                .values({table.c.orientation: random_orientation})
            )
            transaction_conn.execute(stmt)
            print(f"Ažuriran zapis ID: {row.party_id} s orientationom: {random_orientation}")
        except Exception as e:
            print(f"Greška pri ažuriranju ID: {row.party_id} - {e}")
