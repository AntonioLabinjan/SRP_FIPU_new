from pyspark.sql.functions import col, trim, row_number, lit
from pyspark.sql.window import Window
from spark_session import get_spark_session

spark = get_spark_session()
print("Pokrecemo party_dim...")

# --- Hardkodirana orijentacija stranaka ---
party_orientations = {
    1: "left", 2: "right", 3: "center", 4: "right", 5: "center", 
    6: "left", 7: "center", 8: "left", 9: "center", 10: "right", 
    11: "right", 12: "left", 13: "center", 14: "center", 15: "left", 
    16: "left", 17: "center", 18: "right", 19: "center", 20: "center", 
    21: "left"
}

orientation_df = spark.createDataFrame(
    [(k, v) for k, v in party_orientations.items()],
    ["party_id", "orientation"]
)

# --- Glavna funkcija za transformaciju ---
def transform_party_dim(mysql_party_df, csv_party_df=None):
    if mysql_party_df is None:
        raise ValueError("MySQL DataFrame ne smije biti None!")

    print("Ulazni MySQL podaci:")
    mysql_party_df.printSchema()
    mysql_party_df.show(5)

    try:
        mysql_df = (
            mysql_party_df
            .select(
                col("id").cast("int").alias("party_id"),
                trim(col("name")).alias("name")
            )
            .dropna(subset=["party_id", "name"])
            .dropDuplicates(["party_id", "name"])
        )
    except Exception as e:
        raise RuntimeError(f"Greska kod obrade MySQL podataka: {e}")

    # Join s orijentacijama
    try:
        mysql_df = mysql_df.join(orientation_df, on="party_id", how="left")
    except Exception as e:
        raise RuntimeError(f"Greska kod joinanja s orijentacijama: {e}")

    combined_df = mysql_df

    # Ako postoji CSV...
    if csv_party_df is not None:
        print("Ulazni CSV podaci:")
        csv_party_df.printSchema()
        csv_party_df.show(5)

        try:
            csv_clean = (
                csv_party_df
                .select(trim(col("Party")).alias("name"))
                .dropna(subset=["name"])
                .dropDuplicates(["name"])
            )
        except Exception as e:
            raise RuntimeError(f"Greska kod obrade CSV podataka: {e}")

        try:
            window_spec = Window.orderBy("name")
            csv_clean = csv_clean.withColumn("party_id", row_number().over(window_spec) + 1000)
            csv_with_orientation = csv_clean.withColumn("orientation", lit("unknown"))
        except Exception as e:
            raise RuntimeError(f"Greska kod dodavanja ID-a i orijentacije za CSV: {e}")

        try:
            combined_df = (
                mysql_df.unionByName(csv_with_orientation)
                .dropDuplicates(["party_id", "name", "orientation"])
            )
        except Exception as e:
            raise RuntimeError(f"Greska kod kombiniranja MySQL i CSV podataka: {e}")

    # Dodavanje surrogate ključ (party_tk)
    try:
        window = Window.orderBy("party_id", "name")
        final_df = (
            combined_df
            .withColumn("party_tk", row_number().over(window))
            .select("party_tk", "party_id", "name", "orientation")
            .orderBy("party_tk")
        )
    except Exception as e:
        raise RuntimeError(f"Greska kod dodavanja surrogate ključa: {e}")

    print("WOOHOO! Sve je OK!")
    return final_df
