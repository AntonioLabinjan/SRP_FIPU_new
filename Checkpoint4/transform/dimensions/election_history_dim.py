from pyspark.sql.functions import col, trim, to_date, row_number, coalesce, current_timestamp, lit, lead
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window
from datetime import datetime, timedelta

from spark_session import get_spark_session
spark = get_spark_session()

print("Pokrecemo election_history_dim")

def add_date_range_columns(df, start_time="2020-01-01 00:00:00", increment_minutes=60):
    """
    Dodaje 'date_from' i 'date_to' kolone sa inkrementalnim timestampovima.

    :param df: DataFrame kojem dodajemo kolone
    :param start_time: Početni timestamp (str format)
    :param increment_minutes: Koliko minuta se dodaje svakom sljedećem zapisu
    """
    row_count = df.count()
    start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    timestamps = [start + timedelta(minutes=i * increment_minutes) for i in range(row_count)]

    # Stvaramo DataFrame sa timestampovima
    timestamps_df = spark.createDataFrame([(ts,) for ts in timestamps], ["date_from"])

    # Dodajemo redni broj kako bismo mogli spojiti točno po redu
    df_with_index = df.withColumn("row_num", row_number().over(Window.orderBy("election_id")))
    timestamps_df = timestamps_df.withColumn("row_num", row_number().over(Window.orderBy("date_from")))

    # Spajamo originalni DataFrame s timestampovima
    joined_df = df_with_index.join(timestamps_df, on="row_num").drop("row_num")

    # Dodajemo 'date_to' koristeći lead funkciju
    window = Window.orderBy("date_from")
    final_df = joined_df.withColumn(
        "date_to", lead("date_from").over(window)
    ).fillna({"date_to": "9999-12-31 23:59:59"})

    return final_df


def transform_election_history_dim(mysql_election_history_df, csv_election_history_df=None):
    """
    Transformira podatke o izbornoj povijesti iz MySQL i CSV izvora u dimenzijsku tablicu.

    :param mysql_election_history_df: DataFrame s MySQL podacima
    :param csv_election_history_df: (Opcionalno) DataFrame s CSV podacima
    :return: Finalni transformirani DataFrame
    """

    # --- 1. Obrada MySQL podataka ---
    mysql_df = (
        mysql_election_history_df
        .select(
            col("election_id"),
            col("pre_blank_votes"),
            col("pre_null_votes"),
            col("historical_turnout")
        )
        .dropna(subset=["election_id"])  # osiguravamo valjanost zapisa
        .dropDuplicates()
    )

    print(f"Election history dim: MySQL podaci obradeni, broj redaka: {mysql_df.count()}")

    # --- 2. Obrada CSV podataka (ako postoje) ---
    if csv_election_history_df:
        csv_df = (
            csv_election_history_df
            .select(
                col("election_id"),
                col("pre_blank_votes"),
                col("pre_null_votes"),
                col("historical_turnout")
            )
            .dropna(subset=["election_id"])
            .dropDuplicates()
        )

        # Kombiniramo i maknemo duplikate po election_id
        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["election_id"])
    else:
        combined_df = mysql_df

    print(f"Election history dim: Kombinacija MySQL + CSV podataka zavrsena, broj redaka: {combined_df.count()}")

    # --- 3. Dodavanje surrogate key-a i kastanje tipova ---
    window = Window.orderBy("election_id")
    final_df = (
        combined_df
        .withColumn("election_history_tk", row_number().over(window))
        .withColumn("historical_turnout", col("historical_turnout").cast(FloatType()))
        .withColumn("pre_blank_votes", col("pre_blank_votes").cast(IntegerType()))
        .withColumn("pre_null_votes", col("pre_null_votes").cast(IntegerType()))
        .select(
            "election_history_tk",
            "election_id",
            "pre_blank_votes",
            "pre_null_votes",
            "historical_turnout"
        )
        .orderBy("election_id")
    )

    print(f"Election history dim: Dodani surrogate key, broj redaka: {final_df.count()}")

    # --- 4. Dodavanje date_from i date_to ---
    final_df = add_date_range_columns(final_df, start_time="2020-01-01 00:00:00", increment_minutes=60)

    print(f"Election history dim gotov")

    return final_df
