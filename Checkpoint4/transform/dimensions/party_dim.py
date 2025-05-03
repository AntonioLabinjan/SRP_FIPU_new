from pyspark.sql.functions import col, trim, row_number
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_party_dim(mysql_party_df, csv_party_df=None):
    spark = get_spark_session()

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_party_df
        .select(
            col("party_id").cast("int"),
            trim(col("name")).alias("name"),
            trim(col("orientation")).alias("orientation")
        )
        .dropna(subset=["party_id", "name", "orientation"])
        .dropDuplicates(["party_id", "name", "orientation"])
    )

    # --- Step 2: Normalize CSV data (if provided) ---
    if csv_party_df:
        csv_df = (
            csv_party_df
            .select(
                col("party_id").cast("int"),
                trim(col("name")).alias("name"),
                trim(col("orientation")).alias("orientation")
            )
            .dropna(subset=["party_id", "name", "orientation"])
            .dropDuplicates(["party_id", "name", "orientation"])
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["party_id", "name", "orientation"])
    else:
        combined_df = mysql_df

    # --- Step 3: Add surrogate key ---
    window = Window.orderBy("party_id", "name")
    final_df = combined_df.withColumn("party_tk", row_number().over(window)) \
                          .select("party_tk", "party_id", "name", "orientation")

    return final_df.orderBy("party_tk")
