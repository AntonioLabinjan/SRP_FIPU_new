import mysql.connector
import pandas as pd
from datetime import datetime

# Povezivanje na bazu podataka
db_connection = mysql.connector.connect(
    host="localhost",  # Zamijeni s tvojim hostom
    user="root",       # Zamijeni s tvojim korisnikom
    password="root",   # Zamijeni s tvojom lozinkom
    database="dimensional_database"  # Zamijeni s imenom tvoje baze
)

print("otvaram kursor")
cursor = db_connection.cursor()

print("csv ucitan")
# Učitavanje CSV-a u pandas DataFrame
csv_file = 'ElectionData.csv'  # Putanja do tvog CSV-a
df = pd.read_csv(csv_file)

print("radim funkciju za vrijeme")
# Funkcija za konvertiranje vremena u format "00:35:00"
def convert_time_format(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M:%S").strftime("%H:%M:%S")
    except Exception as e:
        return None

# Funkcija za računanje historical_turnout
print("radim turnout funkciju")
def calculate_historical_turnout(total_voters, subscribed_voters):
    if subscribed_voters != 0:
        return (total_voters / subscribed_voters) * 100
    else:
        return None  # Možeš staviti NULL ili 0 ako želiš u slučaju 0 subscribed_voters

# Initialize row_count outside the loop
row_count = 0

print("krece loop")
# Prolazimo kroz svaki red u CSV-u
for index, row in df.iterrows():
    time_elapsed = convert_time_format(row['TimeElapsed'])  # Pretvaranje vremena
    timestamp = datetime.strptime(row['time'], "%Y-%m-%d %H:%M:%S")  # Pretvaranje timestamp-a
    territory_name = row['territoryName']
    total_mandates = row['totalMandates']
    available_mandates = row['availableMandates']
    num_parishes = row['numParishes']
    num_parishes_approved = row['numParishesApproved']
    
    blank_votes = row['blankVotes']
    blank_votes_percentage = row['blankVotesPercentage']
    null_votes = row['nullVotes']
    null_votes_percentage = row['nullVotesPercentage']
    total_voters = row['totalVoters']
    subscribed_voters = row['subscribedVoters']
    voters_percentage = row['votersPercentage']
    
    pre_blank_votes = row['pre.blankVotes']
    pre_null_votes = row['pre.nullVotes']
    #election_id = row['electionId']  # Assuming this is present in the CSV

    # Izračunavanje historical_turnout
    historical_turnout = calculate_historical_turnout(total_voters, subscribed_voters)

    print("u loopu sam, izvodim query")
    # Provjera je li već postoji prethodni zapis za isti teritorij
    select_query = """
    SELECT version, date_to 
    FROM dim_election_history 
    WHERE territory_name = %s AND is_current = TRUE
    ORDER BY version DESC LIMIT 1
    """
    
    cursor.execute(select_query, (territory_name,))
    result = cursor.fetchone()

    if result:
        print("nesto smo nasli")
        print(row_count)
        # Ako postoji prethodni zapis, zaključavamo njegov `date_to` datum
        version, date_to = result
        update_query = """
        UPDATE dim_election_history
        SET date_to = NOW(), is_current = FALSE
        WHERE territory_name = %s AND version = %s
        """
        cursor.execute(update_query, (territory_name, version))
        row_count += 1  # Increment row_count after processing a row
    
    # Umetanje nove verzije podataka
    insert_query = """
    INSERT INTO dim_election_history (
        version, date_from, date_to, territory_name, total_mandates, available_mandates, num_parishes,
        num_parishes_approved, blank_votes, blank_votes_percentage, null_votes, null_votes_percentage, total_voters, 
        subscribed_voters, voters_percentage, pre_blank_votes, pre_null_votes, historical_turnout, is_current
    ) VALUES (
        %s, NOW(), NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE
    )
    """
    
    version = 1  # Postavljanje verzije (ako je ovo prvi unos za teritorij, inače se povećava verzija)
    
    # Parametri koji će biti umetnuti
    data = (
        version, territory_name, total_mandates, available_mandates, num_parishes,
        num_parishes_approved, blank_votes, blank_votes_percentage, null_votes, null_votes_percentage, total_voters, 
        subscribed_voters, voters_percentage, pre_blank_votes, pre_null_votes, historical_turnout
    )

    # Izvršavanje SQL upita za unos podataka
    try:
        cursor.execute(insert_query, data)
        db_connection.commit()  # Potvrda transakcije
    except mysql.connector.Error as err:
        print(f"Greška: {err}")
        db_connection.rollback()  # Povratak na prethodnu transakciju ako dođe do greške

# Print the row_count after processing all rows
print(f"Ukupno je ažurirano {row_count} zapisa.")

# Zatvori konekciju
cursor.close()
db_connection.close()
