from pyspark.sql.functions import col, trim, initcap, row_number
from pyspark.sql.window import Window

print("Pokrecemo elections_fact")

def transform_elections_fact(
    raw_data,
    dim_country_df,
    dim_election_df,
    dim_election_history_df,
    dim_party_df,
    dim_date_df  # Dodajemo dim_date_df kao argument za datum
):
    # Dohvati CSV tablicu (raw CSV input s headerima koje si naveo)
    csv_df = raw_data.get("ElectionData")
    print("Dohvacen election data iz CSV")
    
    if csv_df is None:
        raise ValueError("CSV podaci nisu pronađeni u raw_data!")

    # Očisti i formatiraj CSV podatke
    cleaned_csv = (
        csv_df
        .withColumn("territoryName", initcap(trim(col("territoryName"))))
        .withColumn("Party", initcap(trim(col("Party"))))
        .withColumn("totalMandates", col("totalMandates").cast("int"))
        .withColumn("availableMandates", col("availableMandates").cast("int"))
        .withColumn("numParishes", col("numParishes").cast("int"))
        .withColumn("numParishesApproved", col("numParishesApproved").cast("int"))
        .withColumn("blankVotes", col("blankVotes").cast("int"))
        .withColumn("blankVotesPercentage", col("blankVotesPercentage").cast("float"))
        .withColumn("nullVotes", col("nullVotes").cast("int"))
        .withColumn("nullVotesPercentage", col("nullVotesPercentage").cast("float"))
        .withColumn("votersPercentage", col("votersPercentage").cast("float"))
        .withColumn("subscribedVoters", col("subscribedVoters").cast("int"))
        .withColumn("totalVoters", col("totalVoters").cast("int"))
        .withColumn("Mandates", col("Mandates").cast("int"))
        .withColumn("Percentage", col("Percentage").cast("float"))
        .withColumn("validVotesPercentage", col("validVotesPercentage").cast("float"))
        .withColumn("Votes", col("Votes").cast("int"))
        .withColumn("FinalMandates", col("FinalMandates").cast("int"))
    )
    print("CSV ociscen")

    # Join s dim_party, dim_country, dim_election, dim_election_history i dim_date
    enriched_df = (
        cleaned_csv.alias("c")
        .join(dim_country_df.alias("co"), col("c.territoryName") == col("co.name"), "left")
        .join(dim_party_df.alias("p"), col("c.Party") == col("p.name"), "left")
        .join(dim_election_df.alias("e"), col("co.country_tk") == col("e.country_tk"), "left")
        .join(dim_election_history_df.alias("eh"), col("e.election_id") == col("eh.election_id"), "left")
        .join(dim_date_df.alias("d"), col("c.electionDate") == col("d.date"), "left")  # Join s dim_date
    )
    print("Prva join-sesija OK")

    # Izbor podataka i stvaranje fakt tabele
    fact_df = (
        enriched_df
        .select(
            col("e.election_tk").alias("election_tk"),
            col("p.party_tk").alias("party_tk"),
            col("eh.election_history_tk").alias("election_history_tk"),
            col("c.totalMandates").alias("total_mandates"),
            col("c.availableMandates").alias("available_mandates"),
            col("c.numParishes").alias("num_parishes"),
            col("c.numParishesApproved").alias("num_parishes_approved"),
            col("c.blankVotes").alias("blank_votes"),
            col("c.blankVotesPercentage").alias("blank_votes_percentage"),
            col("c.nullVotes").alias("null_votes"),
            col("c.nullVotesPercentage").alias("null_votes_percentage"),
            col("c.votersPercentage").alias("voters_percentage"),
            col("c.subscribedVoters").alias("subscribed_voters"),
            col("c.totalVoters").alias("total_voters"),
            col("c.Mandates").alias("mandates"),
            col("c.Percentage").alias("percentage"),
            col("c.validVotesPercentage").alias("valid_votes_percentage"),
            col("c.FinalMandates").alias("final_mandates"),
            col("d.date").alias("election_date")  # Dodajemo datum iz dim_date
        )
        .withColumn("fact_id", row_number().over(Window.orderBy("election_tk", "party_tk")))
    )
    print("Fact je gotov")

    return fact_df
