from pyspark.sql.functions import col, trim, row_number, to_date
from pyspark.sql.window import Window
from spark_session import get_spark_session

spark = get_spark_session()

print("pokrecemo election_dim")
def transform_election_dim(mysql_election_df, csv_election_df=None, date_dim_df=None, country_dim_df=None):
    print("usli smo u funkciju transform election dim")

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_election_df
        .select(
            col("id").cast("int"),
            trim(col("year")).alias("election_date_str"),
            col("country_id").cast("bigint")
        )
        .withColumn("election_date", to_date("election_date_str", "yyyy-MM-dd"))
        .dropna(subset=["id", "election_date", "country_id"])
        .dropDuplicates(["id", "election_date", "country_id"])
    )

    # --- Step 2: Normalize CSV data (if provided) ---
    if csv_election_df:
        csv_df = (
            csv_election_df
            .select(
                col("id").cast("int"),
                trim(col("election_date")).alias("election_date_str"),
                col("country_id").cast("bigint")
            )
            .withColumn("election_date", to_date("election_date_str", "yyyy-MM-dd"))
            .dropna(subset=["id", "election_date", "country_id"])
            .dropDuplicates(["id", "election_date", "country_id"])
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["id", "election_date", "country_id"])
    else:
        combined_df = mysql_df

    # --- Step 3: Join with dim_date and dim_country to replace date_id and country_id with surrogate keys ---
    if date_dim_df is not None:
        combined_df = combined_df.join(date_dim_df, combined_df.election_date == date_dim_df.time, "left") \
                                 .withColumn("election_date", date_dim_df.time) \
                                 .drop("time")  # Drop the original date after join

    if country_dim_df is not None:
        combined_df = combined_df.join(country_dim_df, combined_df.country_id == country_dim_df.country_id, "left") \
                                 .withColumn("country_id", country_dim_df.country_id)

    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("id", "election_date")
    final_df = combined_df.withColumn("election_tk", row_number().over(window)) \
                          .select("election_tk", "id", "election_date_str", "country_id")

    # Provjera uspješnosti (ispis broja redaka)
    row_count = final_df.count()
    print(f"WOOHOO! Sve je OK! Broj redaka u finalnom DataFrame-u: {row_count}")

    return final_df.orderBy("election_tk")
