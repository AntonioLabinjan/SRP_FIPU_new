# DIMENSIONAL_DATABASE

import mysql.connector

# Konektiranje na bazu podataka
conn = mysql.connector.connect(
    host="localhost",        # Zamijeni s adresom tvog MySQL servera
    user="root",             # Zamijeni s tvojim MySQL korisnikom
    password="root",     # Zamijeni s tvojom MySQL lozinkom
    database="DIMENSIONAL_DATABASE" # Zamijeni s imenom tvoje baze podataka
)

cursor = conn.cursor()

# Dohvati sve `date_tk` iz dim_date i sve `country_id` iz dim_country
cursor.execute("SELECT date_tk FROM dim_date")
date_tks = cursor.fetchall()

cursor.execute("SELECT country_tk FROM dim_country")
country_tks = cursor.fetchall()

# Početni election_tk i election_id
election_tk = 1
election_id = 1

# Petlja kroz sve date_tk i country_id i insertanje u dim_election
for date_tk in date_tks:
    for country_tk in country_tks:
        # Provjeri da li country_id postoji u dim_country
        cursor.execute("SELECT 1 FROM dim_country WHERE country_tk = %s", (country_tk[0],))
        result = cursor.fetchone()
        
        if result is not None:
            # Unesi podatke u dim_election
            insert_query = """
            INSERT INTO dim_election (election_tk, election_id, date_tk, country_id)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (election_tk, election_id, date_tk[0], country_tk[0]))

            # Povećaj election_tk i election_id
            election_tk += 1
            election_id += 1
        else:
            # Ako country_id ne postoji, preskoči unos
            print(f"Country ID {country_tk[0]} ne postoji u dim_country, preskačem unos.")

# Spremi promjene u bazu
conn.commit()

# Zatvori konekciju
cursor.close()
conn.close()

print("Podaci su uspješno uneseni u dim_election!")

