import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert, select
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from unidecode import unidecode

# -------------------------------
print("Pokrecem ETL skriptu...")
CSV_PATH = "C:/Users/Korisnik/Desktop/SRP_ETL/ElectionData.csv"
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_NAME = 'DIMENSIONAL_DATABASE'
# -------------------------------

DATABASE_URL = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(DATABASE_URL)
conn = engine.connect()
print(" Spojen na bazu.")

metadata = MetaData()
metadata.reflect(bind=engine)
print(f"Reflektirane tablice: {', '.join(metadata.tables.keys())}")

date_table = metadata.tables['dim_date']
country_table = metadata.tables['dim_country']
election_table = metadata.tables['dim_election']

# Učitaj CSV s odgovarajućim kodiranjem
print(f"Ucitavam CSV iz: {CSV_PATH}")
df = pd.read_csv(CSV_PATH, encoding='utf-8-sig')
print(f"Ucitano {len(df)} redaka iz CSV-a.")

# Pretvori vrijeme
print("Parsiram 'time' stupac u datetime...")
df['time'] = pd.to_datetime(df['time'], errors='coerce')
print("Vrijeme parsirano.")

# Normaliziraj datum za mapiranje
df['date_str'] = df['time'].dt.date.astype(str)

# Mapiraj full_date → date_tk
print("Mapiram 'full_date' na 'date_tk'...")
date_map = {
    row.full_date: row.date_tk
    for row in conn.execute(select(date_table.c.full_date, date_table.c.date_tk))
}
print(f"Mapa datuma ucitana ({len(date_map)} vrijednosti).")

df['date_tk'] = df['date_str'].map(date_map)

# Normaliziraj country nazive
df['territoryName_clean'] = df['territoryName'].apply(lambda x: unidecode(str(x)).strip())

# Mapiraj country → country_id
print("Mapiram 'territoryName' na 'country_id'...")
country_map = {
    row.name.strip(): row.country_id
    for row in conn.execute(select(country_table.c.name, country_table.c.country_id))
}
print(f"Mapa zemalja ucitana ({len(country_map)} vrijednosti).")

df['country_id'] = df['territoryName_clean'].map(country_map)

# Prikaži neuspješne mape (dijagnostika)
missing_dates = df['date_tk'].isna().sum()
missing_countries = df['country_id'].isna().sum()
print(f"Nedostaje {missing_dates} 'date_tk' i {missing_countries} 'country_id' vrijednosti.")

unmapped_countries = df[df['country_id'].isna()]['territoryName_clean'].unique()
if len(unmapped_countries) > 0:
    print(f"Države koje nisu mapirane: {unmapped_countries}")

# Izbaci nevažeće redove
df.dropna(subset=['date_tk', 'country_id'], inplace=True)
print(f"Nakon ciscenja ostaje {len(df)} redaka.")

# Raspodijeli timestampove
print("Generiram vremenske oznake (timestampove)...")
start_time = datetime.strptime("18:00:00", "%H:%M:%S")
end_time = datetime.strptime("23:00:00", "%H:%M:%S")

def generate_timestamps(n):
    delta = (end_time - start_time) / max(n - 1, 1)
    return [start_time + i * delta for i in range(n)]

df['timestamp'] = None

for (date_tk, country_id), group in df.groupby(['date_tk', 'country_id']):
    ts_list = generate_timestamps(len(group))
    for idx, (row, row_idx) in enumerate(zip(group.itertuples(), group.index)):
        full_ts = datetime.combine(row.time.date(), ts_list[idx].time())
        df.at[row_idx, 'timestamp'] = full_ts
print("Vremenske oznake generirane.")

# Dohvati max election_tk
print("Dohvacam najveci election_tk...")
max_id_result = conn.execute(select(election_table.c.election_tk).order_by(election_table.c.election_tk.desc()).limit(1)).fetchone()
max_id = max_id_result[0] if max_id_result else 0
print(f"Zadnji election_tk u bazi: {max_id}")

# Ubaci u bazu
print("Ubacujem podatke u dim_election...")
inserted = 0
for idx, row in enumerate(df.itertuples(index=False), start=max_id + 1):
    try:
        conn.execute(
            insert(election_table).values(
                election_tk=idx,
                election_id=idx,
                date_tk=int(row.date_tk),
                country_id=int(row.country_id),
                timestamp=row.timestamp
            )
        )
        inserted += 1
        print(f"Ubacio izbor: TK={idx}, datum={row.date_tk}, država={row.country_id}, vrijeme={row.timestamp.time()}")
    except SQLAlchemyError as e:
        print(f"Greška za red {row}: {e}")

conn.commit()
conn.close()
print(f"Ukupno ubaceno: {inserted} redaka.")
print("Kraj ETL procesa.")
