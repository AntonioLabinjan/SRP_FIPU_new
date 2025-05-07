from pyspark.sql.functions import col, trim, row_number
from pyspark.sql.window import Window
from spark_session import get_spark_session

spark = get_spark_session()

print("Pokrecemo person_dim")

def transform_person_dim(mysql_person_df, csv_person_df=None, party_dim_df=None):

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_person_df
        .select(
            col("id").cast("int"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            col("birth_year").cast("int"),
            trim(col("title")).alias("title"),
            col("party_id").cast("bigint")
        )
        .dropna(subset=["id", "first_name", "last_name", "birth_year", "title", "party_id"])
        .dropDuplicates(["id", "first_name", "last_name", "birth_year", "title", "party_id"])
    )

    print(f"Person dim: MySQL podaci obradeni, broj redaka: {mysql_df.count()}")

    # --- Step 2: Normalize CSV data (if provided) ---
    if csv_person_df:
        csv_df = (
            csv_person_df
            .select(
                col("id").cast("int"),
                trim(col("first_name")).alias("first_name"),
                trim(col("last_name")).alias("last_name"),
                col("birth_year").cast("int"),
                trim(col("title")).alias("title"),
                col("party_id").cast("bigint")
            )
            .dropna(subset=["id", "first_name", "last_name", "birth_year", "title", "party_id"])
            .dropDuplicates(["id", "first_name", "last_name", "birth_year", "title", "party_id"])
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["id", "first_name", "last_name", "birth_year", "title", "party_id"])
    else:
        combined_df = mysql_df

    print(f"Person dim: Kombinacija MySQL + CSV podataka zavrsena, broj redaka: {combined_df.count()}")

    # --- Step 3: Join with party_dim_df to replace party_id with party_tk ---
    if party_dim_df is not None:
        combined_df = combined_df.join(party_dim_df, combined_df.party_id == party_dim_df.party_id, "left") \
                                 .withColumn("party_id", party_dim_df.party_tk) \
                                 .drop("party_tk")  # Drop the original party_tk after join

    print(f"Person dim: Join sa party_dim_df zavrsen, broj redaka: {combined_df.count()}")

    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("id", "first_name", "last_name")
    final_df = combined_df.withColumn("person_tk", row_number().over(window)) \
                          .select("person_tk", "id", "first_name", "last_name", "birth_year", "title", "party_id")

    print(f"Person dim: Dodan surrogate key, broj redaka: {final_df.count()}")

    return final_df.orderBy("person_tk")
