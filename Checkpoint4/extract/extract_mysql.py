# extract/extract_mysql.py
from spark_session import get_spark_session

def extract_table(table_name):
    spark = get_spark_session("ETL_App")


    jdbc_url = "jdbc:mysql://127.0.0.1:3306/dw?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    return df

def extract_all_tables():
    return {
        "country": extract_table("country"),
        "election": extract_table("election"),
        "election_history": extract_table("election_history"),
        "party": extract_table("party"),
        "person": extract_table("person"),
        "result": extract_table("result"),
        
    }