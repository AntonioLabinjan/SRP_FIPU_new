from pyspark.sql import DataFrame

print("pokrecemo run_loading.py")
def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/zadnji?useSSL=false" 
    print(jdbc_url)
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print(f"Writing to table `{table_name}` with mode `{mode}`...")
    spark_df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=connection_properties
    )
    print(f"✅ Done writing to `{table_name}`.")