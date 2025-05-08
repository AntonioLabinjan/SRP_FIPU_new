import random
from sqlalchemy import create_engine, MetaData, Table, insert, select

# -------------------------------
# 🔧 KONFIGURACIJA – prilagodi ovo
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

# Učitavanje tablica
party_table = Table('dim_party', metadata, autoload_with=engine)
person_table = Table('dim_person', metadata, autoload_with=engine)

# Dohvati sve stranke
party_rows = conn.execute(select(party_table.c.party_tk, party_table.c.name)).fetchall()

# Portugalska imena s pripadajućim spolom
male_names = ['Carlos', 'João', 'Pedro', 'Rui', 'Miguel', 'Tiago', 'André', 'Manuel']
female_names = ['Maria', 'Ana', 'Inês', 'Beatriz', 'Sofia', 'Cláudia', 'Joana', 'Marta']
last_names = ['Silva', 'Santos', 'Ferreira', 'Pereira', 'Oliveira', 'Costa', 'Rodrigues', 'Martins', 'Gomes', 'Lopes']
titles_male = ['Dr.', 'Eng.', 'Prof.', 'Sr.']
titles_female = ['Dra.', 'Eng.ª', 'Prof.ª', 'Sra.']

# Funkcija za generaciju osobe sa spolom
def generate_real_person(party_tk, counter):
    gender = random.choice(['M', 'F'])
    if gender == 'M':
        first_name = random.choice(male_names)
        title = random.choice(titles_male)
    else:
        first_name = random.choice(female_names)
        title = random.choice(titles_female)

    return {
        'person_tk': counter,
        'person_id': counter,
        'first_name': first_name,
        'last_name': random.choice(last_names),
        'birth_year': random.randint(1950, 2000),
        'title': title,
        'gender': gender,
        'party_id': party_tk
    }

# Ubacivanje podataka
counter = 1
for party in party_rows:
    new_person = generate_real_person(party.party_tk, counter)
    conn.execute(insert(person_table).values(new_person))
    print(f"Dodana osoba za stranku {party.name}: {new_person['first_name']} {new_person['last_name']} ({new_person['gender']})")
    counter += 1

conn.commit()
conn.close()
