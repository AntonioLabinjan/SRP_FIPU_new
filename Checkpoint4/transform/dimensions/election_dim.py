from pyspark.sql.functions import col, trim, row_number, to_date
from pyspark.sql.window import Window
from spark_session import get_spark_session


def transform_election_dim(mysql_election_df, csv_election_df=None, date_dim_df=None, country_dim_df=None):
    spark = get_spark_session()

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_election_df
        .select(
            col("election_id").cast("int"),
            trim(col("election_date")).alias("election_date_str"),
            col("country_id").cast("bigint")
        )
        .withColumn("election_date", to_date("election_date_str", "yyyy-MM-dd"))
        .dropna(subset=["election_id", "election_date", "country_id"])
        .dropDuplicates(["election_id", "election_date", "country_id"])
    )

    # --- Step 2: Normalize CSV data (if provided) ---
    if csv_election_df:
        csv_df = (
            csv_election_df
            .select(
                col("election_id").cast("int"),
                trim(col("election_date")).alias("election_date_str"),
                col("country_id").cast("bigint")
            )
            .withColumn("election_date", to_date("election_date_str", "yyyy-MM-dd"))
            .dropna(subset=["election_id", "election_date", "country_id"])
            .dropDuplicates(["election_id", "election_date", "country_id"])
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["election_id", "election_date", "country_id"])
    else:
        combined_df = mysql_df

    # --- Step 3: Join with dim_date and dim_country to replace date_id and country_id with surrogate keys ---
    if date_dim_df is not None:
        combined_df = combined_df.join(date_dim_df, combined_df.election_date == date_dim_df.full_date, "left") \
                                 .withColumn("date_tk", date_dim_df.date_tk) \
                                 .drop("full_date")  # Drop the original date after join

    if country_dim_df is not None:
        combined_df = combined_df.join(country_dim_df, combined_df.country_id == country_dim_df.country_id, "left") \
                                 .withColumn("country_tk", country_dim_df.country_tk) \
                                 .drop("country_id")  # Drop the original country_id after join

    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("election_id", "election_date")
    final_df = combined_df.withColumn("election_tk", row_number().over(window)) \
                          .select("election_tk", "election_id", "date_tk", "country_tk")

    return final_df.orderBy("election_tk")
