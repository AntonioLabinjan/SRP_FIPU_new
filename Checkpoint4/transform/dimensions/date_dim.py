from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, date_format, row_number, trim
)
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from spark_session import get_spark_session

spark = get_spark_session()
print("Pokrecemo date_dim...")
def normalize_dates(df: DataFrame, source_name="unknown") -> DataFrame:
    possible_date_columns = ["full_date", "time"]
    date_column = None
    
    for col_name in possible_date_columns:
        if col_name in df.columns:
            date_column = col_name
            break

    if not date_column:
        raise ValueError(f"[{source_name}] Nije pronađena kolona s datumom. Dostupne kolone: {df.columns}")
    
    print(f"[{source_name}] Koristimo kolonu '{date_column}' za datum...")

    return (
        df
        .select(trim(col(date_column)).alias("full_date_str"))
        .withColumn("full_date", to_timestamp("full_date_str", "yyyy-MM-dd HH:mm:ss"))
        .dropna(subset=["full_date"])
        .dropDuplicates(["full_date"])
    )


def transform_date_dim(mysql_date_df: DataFrame = None, csv_date_df: DataFrame = None) -> DataFrame:
    print("Zapocinjemo transformaciju date_dim...")

    sources = []

    if mysql_date_df is not None:
        sources.append(normalize_dates(mysql_date_df, "MySQL"))

    if csv_date_df is not None:
        sources.append(normalize_dates(csv_date_df, "CSV"))

    if not sources:
        raise ValueError("Nije dostavljen nijedan izvor podataka (ni MySQL ni CSV).")

    # Kombinacija izvora i deduplikacija
    combined_df = sources[0]
    for src in sources[1:]:
        combined_df = combined_df.unionByName(src).dropDuplicates(["full_date"])

    print(f"Ukupno razlicitih datuma: {combined_df.count()}")

    # Ekstrakcija dimenzija datuma
    enriched_df = (
        combined_df
        .withColumn("year", year("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("weekday", date_format("full_date", "EEEE"))
        .withColumn("time", date_format("full_date", "HH:mm:ss"))
    )

    # Surrogate key
    window = Window.orderBy("full_date")
    final_df = (
        enriched_df.withColumn("date_tk", row_number().over(window))
                   .select("date_tk", "full_date", "year", "month", "day", "weekday", "time")
                   .orderBy("full_date")
    )

    print("Transformacija zavrsena.")
    return final_df
