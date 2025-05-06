from pyspark.sql.functions import col, trim, initcap, lit, row_number, monotonically_increasing_id, when
from pyspark.sql.window import Window
import random
from spark_session import get_spark_session
spark = get_spark_session()

print("pokrecemo country_dim")


def transform_country_dim(mysql_country_df, csv_country_df=None):
    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_country_df
        .select(
            col("id").cast("long").alias("country_id"),
            initcap(trim(col("name"))).alias("name"),
            initcap(trim(col("region"))).alias("region")
        )
        .dropDuplicates(["name"])
    )

    # --- Step 2: Normalize CSV data (if exists) ---
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

    # --- Step 3: Drop duplicates by name (precaution) ---
    combined_df = combined_df.dropDuplicates(["name"])

    # --- Step 4: Fill in missing region values with random ones ---
    regions = ["Center", "East", "South", "North", "West"]
    region_list = [random.choice(regions) for _ in range(combined_df.count())]
    region_df = spark.createDataFrame([(r,) for r in region_list], ["region_random"])

    combined_df = combined_df.withColumn("temp_id", monotonically_increasing_id())
    region_df = region_df.withColumn("temp_id", monotonically_increasing_id())


    from pyspark.sql.functions import expr

# Direktno dodavanje random regiona u Spark DataFrame
    combined_df = (
        combined_df
        .withColumn("region", when(col("region").isNotNull(), col("region"))
                .otherwise(expr(f"element_at(array({','.join([f'\"{r}\"' for r in regions])}), cast(rand() * {len(regions)} + 1 as int))")))
    )


    # --- Step 5: Add surrogate key ---
    window = Window.orderBy("region", "name")
    final_df = (
        combined_df
        .withColumn("country_tk", row_number().over(window))
        .select("country_tk", "country_id", "name", "region")
    )

    # --- Step 6: Print number of rows ---



    try:
        print("Broj redaka u dimenziji drzava:", final_df.count())
    except Exception as e:
        print("Greska pri brojanju redova:", str(e))
        final_df.show(5)
        final_df.printSchema()


    # --- Step 7: Assert expected count (can adjust or comment if dynamic) ---

    print(" Final country_dim rows:", final_df.count())

    return final_df
