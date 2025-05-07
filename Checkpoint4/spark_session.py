### TRIED TO DEBUG IT, BUT IT NO WORK MADONNA MIA!!!!!!!!!!!! => SAD VALJDA DELA...ne znan
from pyspark.sql import SparkSession
import os

print("Pokrecemo Spark session...")

import os

# Hardkodiraj path do Jave koju koristi Spark => NI POMOGLO :)



os.environ['JAVA_HOME'] = r"C:\Java\jdk-11\openjdk-11.0.2_windows-x64_bin\jdk-11.0.2"
os.environ['PATH'] = os.environ['JAVA_HOME'] + r'\bin;' + os.environ['PATH']




def get_spark_session(app_name="ETL_App"):
    print("Trying to start Spark session...")
    print("3")
    print("2")
    print("1")
    
    # Lokalni path za provjeru postojanja filea
    jar_file_local = r"C:\Users\Korisnik\Desktop\Skladista_rudarenje_podataka_projekt-master\Connectors\mysql-connector-j-9.2.0.jar"

    if os.path.exists(jar_file_local):
        print("JAR file found on the specified path.")
    else:
        print("Error: JAR file not found at the specified path.")
        return None

    # URI path za Spark
    jar_file_uri = "file:///" + jar_file_local.replace("\\", "/")

    print(f"Starting Spark session with the following configuration:")
    print(f"App name: {app_name}")
    print(f"JAR path: {jar_file_uri}")

    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", jar_file_uri) \
            .getOrCreate()
        print("Spark session successfully started.")
        print(f"Spark version: {spark.version}")
        return spark
    except Exception as e:
        print("Failed to start Spark session:")
        print(str(e))
        return None

