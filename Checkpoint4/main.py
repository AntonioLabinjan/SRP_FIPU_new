# main.py
from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_spark_df_to_mysql
import os
from pyspark.sql.functions import to_timestamp





print("pokrenuli smo main.py")
# Unset SPARK_HOME if it exists to prevent Spark session conflicts
os.environ.pop("SPARK_HOME", None)
print("ovo je ok")



def main():
    
  

    print("usli smo u main")
    spark = get_spark_session()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    if spark is None:
        print("Spark session failed to initialize. Exiting.")
        exit(1)
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # Load data
    print("Starting data extraction")
    mysql_df = extract_all_tables()
    print("path for csv trying to be found")
    csv_df = {"csv_sales":extract_from_csv("C:/Users/Korisnik/Desktop/Skladista_rudarenje_podataka_projekt-master/ElectionData_PROCESSED_20.csv")}
    merged_df = {**mysql_df, **csv_df}
    print("Data extraction completed")

    # Transform data
    print("Starting data transformation")
    load_ready_dict = run_transformations(merged_df)
    print("Data transformation completed")

    # Load data
    print("Starting data loading")
    for table_name, df in load_ready_dict.items():
        write_spark_df_to_mysql(df, table_name)
    print("Data loading completed")

if __name__ == "__main__":
    main()