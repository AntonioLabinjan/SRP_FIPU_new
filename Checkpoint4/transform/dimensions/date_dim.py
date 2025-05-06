from pyspark.sql.functions import (col, trim, to_timestamp, year, month, dayofmonth, date_format, row_number)
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from spark_session import get_spark_session
from pyspark.sql import DataFrame

def normalize_time_df(df: DataFrame, source_name="unknown") -> DataFrame:
    print(f"[normalize_time_df] Obrada izvora: {source_name}")
    
    possible_date_columns = ["time", "year"]
    date_column = next((col_name for col_name in possible_date_columns if col_name in df.columns), None)

    if not date_column:
        raise ValueError(f"[{source_name}] Nema prepoznatljive kolone s datumom. Dostupno: {df.columns}")

    df = (
        df
        .withColumn("time_str", trim(col(date_column)))
        .withColumn("time", to_timestamp("time_str", "yyyy-MM-dd HH:mm:ss"))
        .dropna(subset=["time"])
        .dropDuplicates(["time"])
    )

    null_count = df.filter(col("time").isNull()).count()
    if null_count > 0:
        print(f"[{source_name}] {null_count} neuspjesno parsiranih redova (NULL)")

    print(f"[{source_name}] Validnih redova: {df.count()}")
    return df.select("time")

def transform_date_dim(mysql_date_df: DataFrame, csv_date_df: DataFrame = None) -> DataFrame:
    spark = get_spark_session()
    print("Pokrecemo transformaciju date_dim")

    sources = []

    if mysql_date_df is not None:
        sources.append(normalize_time_df(mysql_date_df, "MySQL"))
    if csv_date_df is not None:
        sources.append(normalize_time_df(csv_date_df, "CSV"))

    if not sources:
        raise ValueError("Nema dostupnih izvora (MySQL ni CSV)")

    combined_df = sources[0]
    for other_df in sources[1:]:
        combined_df = combined_df.unionByName(other_df).dropDuplicates(["time"])

    # Obogacivanje s datumskim dimenzijama
    enriched_df = (
        combined_df
        .withColumn("year", year("time"))
        .withColumn("month", month("time"))
        .withColumn("day", dayofmonth("time"))
        .withColumn("weekday", date_format("time", "EEEE"))
    )

    # Generiranje surrogate key-a
    window = Window.orderBy("time")
    final_df = (
        enriched_df
        .withColumn("date_tk", row_number().over(window))
        .select("date_tk", "time", "year", "month", "day", "weekday")
        .orderBy("time")
    )

    print(f"Transformacija zavrsena. Ukupan broj redova: {final_df.count()}")
    return final_df


'''from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, date_format, row_number, trim
)
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from spark_session import get_spark_session

# Inicijalizacija Spark sesije
spark = get_spark_session()
print("Pokrecemo date_dim skriptu...")

# Normalizacija datuma iz zadanog DataFrame-a
def normalize_dates(df: DataFrame, source_name="unknown") -> DataFrame:
    print(f"[normalize_dates] Pokrenuta za izvor: {source_name}")
    print(f"[normalize_dates] Ulazne kolone: {df.columns}\n")

    possible_date_columns = ["time", "year"]  # Dodajemo 'time' kao moguću kolonu za datum
    date_column = None

    for col_name in possible_date_columns:
        if col_name in df.columns:
            date_column = col_name
            break

    if not date_column:
        raise ValueError(f"[{source_name}] Nije pronadena kolona s datumom. Dostupne kolone: {df.columns}")

    print(f"[{source_name}] Koristimo kolonu '{date_column}' za datum...")

    # Pretvaranje stringa u timestamp
    cleaned_df = (
        df
        .withColumn("time_str", trim(col(date_column)))  # Uklanjanje nepotrebnih razmaka
        .withColumn("time", to_timestamp("time_str", "yyyy-MM-dd HH:mm:ss"))  # Parsiranje datuma
    )

    print(f" [{source_name}] Primjer parsiranih vrijednosti:")
    cleaned_df.select("time_str", "time").show(5, truncate=False)

    # Provjera neuspješnog parsiranja
    null_dates = cleaned_df.filter(col("time").isNull())
    count_nulls = null_dates.count()
    if count_nulls > 0:
        print(f"\n[{source_name}] Upozorenje: {count_nulls} redova NIJE uspjesno parsirano kao datum: ")
        null_dates.select("time_str").show(truncate=False)

    valid_count = cleaned_df.filter(col("time").isNotNull()).count()
    print(f"[{source_name}] Validnih datuma: {valid_count}, Neispravnih: {count_nulls}")

    final_df = cleaned_df.select("time")

    print(f"[{source_name}] Broj jedinstvenih datuma nakon ciscenja: {final_df.count()}")
    return final_df


# Glavna transformacijska funkcija za dimenziju datuma
def transform_date_dim(time: DataFrame = None, csv_date_df: DataFrame = None) -> DataFrame:
    print("\nPocinjemo transformaciju `date_dim`...\n")

    sources = []

    # Provjera i normalizacija izvora
    if time is not None:
        print(" Pristigao MySQL DataFrame...")
        sources.append(normalize_dates(time, "MySQL"))
    else:
        print(" MySQL DataFrame nije dostavljen.")

    if csv_date_df is not None:
        print(" Pristigao CSV DataFrame...")
        sources.append(normalize_dates(csv_date_df, "CSV"))
    else:
        print(" CSV DataFrame nije dostavljen.")

    if not sources:
        raise ValueError("Nije dostavljen nijedan izvor podataka (ni MySQL ni CSV).")

    print(" Spajanje izvora i deduplikacija...")
    combined_df = sources[0]
    for src in sources[1:]:
        print(" Dodajemo još jedan izvor...")
        combined_df = combined_df.unionByName(src).dropDuplicates(["time"])

    # Uklanjanje NULL datuma
    combined_df = combined_df.dropna(subset=["time"])
    total_unique_dates = combined_df.count()
    print(f" Ukupno jedinstvenih datuma nakon spajanja: {total_unique_dates}")

    print(" Dodajemo dimenzije (godina, mjesec, dan, dan u tjednu)...")
    enriched_df = (
        combined_df
        .withColumn("year", year("time"))
        .withColumn("month", month("time"))
        .withColumn("day", dayofmonth("time"))
        .withColumn("weekday", date_format("time", "EEEE"))
    )

    print(" Primjer obogacenih redova:")
    enriched_df.show(5, truncate=False)

    print(" Generiramo surrogate key (date_tk)...")
    window = Window.orderBy("time")
    final_df = (
        enriched_df
        .withColumn("date_tk", row_number().over(window))
        .select("date_tk", "time", "year", "month", "day", "weekday")
        .orderBy("time")
    )

    print(f" Transformacija zavrsena. Ukupan broj redova u finalnom DataFrameu: {final_df.count()}")
    print(" Prvih par redova finalne dimenzije:")
    final_df.show(10, truncate=False)

    return final_df'''
