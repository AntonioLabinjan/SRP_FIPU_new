from pyspark.sql.functions import col, trim, initcap, lit, row_number
from pyspark.sql.window import Window

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

    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("region", "name")

    final_df = (
        combined_df
        .withColumn("country_tk", row_number().over(window))
        .select("country_tk", "country_id", "name", "region")
    )

    # --- Step 5: Assert expected count ---
    assert final_df.count() == 21, "Expected 21 countries in final dimension table."

    return final_df
