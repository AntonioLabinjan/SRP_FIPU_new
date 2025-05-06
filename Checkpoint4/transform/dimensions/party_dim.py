from pyspark.sql.functions import col, trim, row_number, udf
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from spark_session import get_spark_session

spark = get_spark_session()

print("pokrecemo party_dim")

# --- Hardkodirana orijentacija stranaka ---
party_orientations = {
    1: "left",   # PS
    2: "right",  # PPD/PSD
    3: "center", # B.E.
    4: "right",  # CDS-PP
    5: "center", # PAN
    6: "left",   # PCTP/MRPP
    7: "center", # A
    8: "left",   # L
    9: "center", # JPP
    10: "right", # PDR
    11: "right", # PNR
    12: "left",  # PURP
    13: "center",# PPM
    14: "center",# MPT
    15: "left",  # MAS
    16: "left",  # PCP-PEV
    17: "center",# R.I.R.
    18: "right", # CH
    19: "center",# IL
    20: "center",# NC
    21: "left",  # PTP
}

# --- UDF za dohvat orijentacije po ID-u ---
def get_orientation_by_id(party_id):
    return party_orientations.get(party_id, "unknown")

get_orientation_udf = udf(get_orientation_by_id, StringType())

# --- Glavna funkcija za transformaciju ---
def transform_party_dim(mysql_party_df, csv_party_df=None):

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_party_df
        .select(
        col("id").cast("int").alias("party_id"),
        trim(col("name")).alias("name")
)
        .dropna(subset=["party_id", "name"])
        .dropDuplicates(["party_id", "name"])
        .withColumn("orientation", get_orientation_udf(col("party_id")))
    )

    # --- Step 2: Normalize CSV data (if provided) ---
    from pyspark.sql.functions import monotonically_increasing_id
    
    if csv_party_df:
        csv_df = (
            csv_party_df
            .select(
                trim(col("Party")).alias("name")
            )
            .dropna(subset=["name"])
            .dropDuplicates(["name"])
            .withColumn("party_id", monotonically_increasing_id().cast("int"))  # Generiramo ID
            .withColumn("orientation", get_orientation_udf(col("party_id")))
        )

        # Combine MySQL and CSV data
        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["party_id", "name", "orientation"])
    else:
        combined_df = mysql_df

    # --- Step 3: Add surrogate key ---
    window = Window.orderBy("party_id", "name")
    final_df = combined_df.withColumn("party_tk", row_number().over(window)) \
                          .select("party_tk", "party_id", "name", "orientation")

    # Provjera uspješnosti (print broja redaka)
    row_count = final_df.count()
    print(f"WOOHOO! Sve je OK! Broj redaka u finalnom DataFrame-u: {row_count}")

    return final_df.orderBy("party_tk")
