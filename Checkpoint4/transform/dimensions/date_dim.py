from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, date_format, row_number, trim
)
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_date_dim(mysql_date_df, csv_date_df=None):
    spark = get_spark_session()

    # --- Step 1: Normalize MySQL data (with timestamp) ---
    mysql_df = (
        mysql_date_df
        .select(trim(col("full_date")).alias("full_date_str"))
        .withColumn("full_date", to_timestamp("full_date_str", "yyyy-MM-dd HH:mm:ss"))
        .dropna(subset=["full_date"])
        .dropDuplicates(["full_date"])
    )

    # --- Step 2: Normalize CSV data (if provided) ---
    if csv_date_df:
        csv_df = (
            csv_date_df
            .select(trim(col("full_date")).alias("full_date_str"))
            .withColumn("full_date", to_timestamp("full_date_str", "yyyy-MM-dd HH:mm:ss"))
            .dropna(subset=["full_date"])
            .dropDuplicates(["full_date"])
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["full_date"])
    else:
        combined_df = mysql_df

    # --- Step 3: Extract year, month, day, weekday, time ---
    combined_df = combined_df \
        .withColumn("year", year("full_date")) \
        .withColumn("month", month("full_date")) \
        .withColumn("day", dayofmonth("full_date")) \
        .withColumn("weekday", date_format("full_date", "EEEE")) \
        .withColumn("time", date_format("full_date", "HH:mm:ss"))

    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("full_date")
    final_df = combined_df.withColumn("date_tk", row_number().over(window)) \
                          .select("date_tk", "full_date", "year", "month", "day", "weekday", "time")

    return final_df.orderBy("full_date")
