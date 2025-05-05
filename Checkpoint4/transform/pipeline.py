from transform.dimensions.country_dim import transform_country_dim
from transform.dimensions.date_dim import transform_date_dim
from transform.dimensions.party_dim import transform_party_dim
from transform.dimensions.election_dim import transform_election_dim
from transform.dimensions.election_history_dim import transform_election_history_dim
from transform.dimensions.person_dim import transform_person_dim
from transform.facts.elections_fact import transform_elections_fact  

print(" Pokrecemo pipeline.py")

def run_transformations(raw_data):
    print("\n Starting all transformations...\n")

    # 📌 1. Country dimension
    print(" [1] Transforming Country dimension...")
    try:
        country_dim = transform_country_dim(
            raw_data["country"],
            csv_country_df=raw_data.get("ElectionData")
        )
        print("[1] Country dimension complete\n")
    except Exception as e:
        print(f" [1] Country dimension failed: {e}")
        raise

    # 📌 2. Date dimension
    '''
    print(" [2] Transforming Date dimension...")
    try:
        date_dim = transform_date_dim(
            raw_data["time"],
            csv_date_df=raw_data.get("ElectionData")
        )
        print(" [2] Date dimension complete\n")
    except Exception as e:
        print(f" [2] Date dimension failed: {e}")
        raise
'''
    # 📌 3. Party dimension
    print(" [3] Transforming Party dimension...")
    try:
        party_dim = transform_party_dim(
            raw_data["party"],
            csv_party_df=raw_data.get("ElectionData")
        )
        print(" [3] Party dimension complete\n")
    except Exception as e:
        print(f" [3] Party dimension failed: {e}")
        raise

    # 📌 4. Election dimension
    print(" [4] Transforming Election dimension...")
    try:
        election_dim = transform_election_dim(
            raw_data["election"],
            csv_election_df=raw_data.get("ElectionData")
        )
        print(" [4] Election dimension complete\n")
    except Exception as e:
        print(f" [4] Election dimension failed: {e}")
        raise

    # 📌 5. Election History dimension
    print(" [5] Transforming Election History dimension...")
    try:
        election_history_dim = transform_election_history_dim(
            raw_data["election_history"],
            csv_election_history_df=raw_data.get("ElectionData")
        )
        print(" [5] Election History dimension complete\n")
    except Exception as e:
        print(f" [5] Election History dimension failed: {e}")
        raise

    # 📌 6. Person dimension
    print(" [6] Transforming Person dimension...")
    try:
        person_dim = transform_person_dim(
            raw_data["person"],
            csv_person_df=raw_data.get("ElectionData")
        )
        print(" [6] Person dimension complete\n")
    except Exception as e:
        print(f" [6] Person dimension failed: {e}")
        raise

    # 📌 7. Fact table
    print(" [7] Transforming Election Data fact table...")
    try:
        election_data_fact = transform_elections_fact(
            raw_data,
            country_dim,
            date_dim,
            party_dim,
            election_dim,
            election_history_dim,
            person_dim
        )
        print(" [7] Election Data fact table complete\n")
    except Exception as e:
        print(f" [7] Election Data fact table failed: {e}")
        raise

    print(" All transformations completed successfully!")

    return {
        "dim_country": country_dim,
        "dim_date": date_dim,
        "dim_party": party_dim,
        "dim_election": election_dim,
        "dim_election_history": election_history_dim,
        "dim_person": person_dim,
        "fact_election_data": election_data_fact
    }
