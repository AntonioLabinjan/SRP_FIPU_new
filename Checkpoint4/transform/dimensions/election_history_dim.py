from pyspark.sql.functions import col, trim, to_date, row_number, coalesce, current_timestamp, lit
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window


from spark_session import get_spark_session
spark = get_spark_session()


print("pokrecemo election_history_dim")
def transform_election_history_dim(mysql_election_history_df, csv_election_history_df=None):
    

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_election_history_df
        .select(
            col("election_id"),
            col("version"),
            col("date_from"),
            col("date_to"),
            col("pre_blank_votes"),
            col("pre_null_votes"),
            col("historical_turnout")
        )
        .dropna(subset=["election_id", "date_from"])  # Ensure valid records
        .dropDuplicates()
    )
    print("Election history dim napreduje")
    # --- Step 2: Normalize CSV data (if provided) ---
    if csv_election_history_df:
        csv_df = (
            csv_election_history_df
            .select(
                col("election_id"),
                col("version"),
                col("date_from"),
                col("date_to"),
                col("pre_blank_votes"),
                col("pre_null_votes"),
                col("historical_turnout")
            )
            .dropna(subset=["election_id", "date_from"])  # Ensure valid records
            .dropDuplicates()
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["election_id", "date_from"])
    else:
        combined_df = mysql_df
    print("ide i dalje")
    # --- Step 3: Add surrogate key ---
    window = Window.orderBy("election_id", "date_from")
    final_df = combined_df.withColumn("election_history_tk", row_number().over(window)) \
                          .withColumn("date_from", to_date("date_from", "yyyy-MM-dd")) \
                          .withColumn("date_to", to_date("date_to", "yyyy-MM-dd")) \
                          .withColumn("version", coalesce(col("version"), lit(1))) \
                          .withColumn("historical_turnout", col("historical_turnout").cast(FloatType())) \
                          .select("election_history_tk", "version", "date_from", "date_to", 
                                  "election_id", "pre_blank_votes", "pre_null_votes", "historical_turnout")

    return final_df.orderBy("date_from")
