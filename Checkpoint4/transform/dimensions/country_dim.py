from pyspark.sql.functions import col, trim, initcap, lit, row_number, when, expr
from pyspark.sql.window import Window
from spark_session import get_spark_session

spark = get_spark_session()
print("pokrecemo country_dim")

def transform_country_dim(mysql_country_df, csv_country_df=None, jdbc_url=None, connection_properties=None):
    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_country_df
        .select(
    col("id").cast("bigint").alias("country_id"),
    initcap(trim(col("name"))).alias("country_name"),  # ✅ točno!
    initcap(trim(col("region"))).alias("region")
)

        .dropDuplicates(["country_name"])
    )

    # --- Step 2: Normalize CSV data (ako postoji) ---
    if csv_country_df:
        csv_df = (
            csv_country_df
            .select(
                initcap(trim(col("name"))).alias("name")
            )
            .withColumn("country_id", lit(None).cast("long"))
            .withColumn("region", lit(None).cast("string"))
            .dropDuplicates(["name"])
        )
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df

    # --- Step 3: Ukloni postojeće country_id-ove iz baze ---
    if jdbc_url and connection_properties:
        try:
            existing_ids_df = spark.read.jdbc(url=jdbc_url, table="dim_country", properties=connection_properties)
            existing_ids = [row["country_id"] for row in existing_ids_df.select("country_id").dropna().collect()]
            if existing_ids:
                combined_df = combined_df.filter(~col("country_id").isin(existing_ids))
                print(f"Filtrirano {len(existing_ids)} već postojećih country_id-ova.")
            else:
                print("Nema postojećih country_id-ova za filtrirati.")
        except Exception as e:
            print(f"Greška pri dohvaćanju postojećih ID-eva: {e}")

    # --- Step 4: Drop duplikate po imenu ---
    combined_df = combined_df.dropDuplicates(["country_name"])

    # --- Step 5: Drop duplikate po country_id ako postoji više istih ---
    combined_df = (
        combined_df
        .withColumn("dup_flag", when(col("country_id").isNull(), lit(-1)).otherwise(col("country_id")))
        .dropDuplicates(["dup_flag"])
        .drop("dup_flag")
    )

    # --- Step 6: Popuni region ako nedostaje ---
    regions = ["Center", "East", "South", "North", "West"]
    combined_df = (
        combined_df
        .withColumn(
            "region",
            when(col("region").isNotNull(), col("region"))
            .otherwise(expr(f"element_at(array({','.join([f'\"{r}\"' for r in regions])}), cast(rand() * {len(regions)} + 1 as int))"))
        )
    )

    # --- Step 7: Dodaj surrogate key ---
    window = Window.orderBy("region", "country_name")
    final_df = (
        combined_df
        .withColumn("country_tk", row_number().over(window))
        .select("country_tk", "country_id", "country_name", "region")
    )

    # --- Step 8: Drop duplikate po country_id ako postoji ---
    final_df = final_df.withColumn("rownum", row_number().over(Window.partitionBy("country_id").orderBy("country_tk")))
    final_df = final_df.filter((col("country_id").isNull()) | (col("rownum") == 1)).drop("rownum")

    # --- Step 9: Print broj redaka ---
    try:
        print("Broj redaka u dimenziji drzava:", final_df.count())
    except Exception as e:
        print("Greska pri brojanju redova:", str(e))
        final_df.show(5)
        final_df.printSchema()

    print("Final country_dim rows:", final_df.count())

    return final_df
