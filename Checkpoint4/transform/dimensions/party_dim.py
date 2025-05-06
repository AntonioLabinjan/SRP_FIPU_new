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
    print("Ulazni MySQL podaci:")
    mysql_party_df.printSchema()
    mysql_party_df.show(5)

    # Step 1: Normalize MySQL data
    mysql_df = (
        mysql_party_df
        .select(
            col("id").cast("int").alias("party_id"),
            trim(col("name")).alias("name")
        )
        .dropna(subset=["party_id", "name"])
        .dropDuplicates(["party_id", "name"])
    )

    # Join s orijentacijama
    mysql_df = mysql_df.join(orientation_df, on="party_id", how="left")

    combined_df = mysql_df

    # Step 2: Ako postoji CSV
    if csv_party_df:
        print("Ulazni CSV podaci:")
        csv_party_df.printSchema()
        csv_party_df.show(5)

        csv_clean = (
            csv_party_df
            .select(trim(col("Party")).alias("name"))
            .dropna(subset=["name"])
            .dropDuplicates(["name"])
        )

        # Dodaj party_id s offsetom
        window_spec = Window.orderBy("name")
        csv_clean = csv_clean.withColumn("party_id", row_number().over(window_spec) + 1000)

        # Dodaj orijentaciju: default = unknown
        csv_with_orientation = (
            csv_clean
            .withColumn("orientation", lit("unknown"))
        )

        # Kombinacija s MySQL strankama
        combined_df = (
            mysql_df.unionByName(csv_with_orientation)
            .dropDuplicates(["party_id", "name", "orientation"])
        )

    # Step 3: Dodaj surrogate ključ (party_tk)
    window = Window.orderBy("party_id", "name")
    final_df = (
        combined_df
        .withColumn("party_tk", row_number().over(window))
        .select("party_tk", "party_id", "name", "orientation")
        .orderBy("party_tk")
    )

    print("WOOHOO! Sve je OK!")
    # final_df.show(10, truncate=False)
    return final_df
