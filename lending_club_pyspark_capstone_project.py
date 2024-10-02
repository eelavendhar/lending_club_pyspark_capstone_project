from pyspark.sql import SparkSession

# Create a Spark session in local mode
spark = SparkSession.builder \
    .appName("lending_club_pyspark_capstone_project") \
    .enableHiveSupport() \
    .getOrCreate()    

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Input dataset path
input_dataset = "data/input_dataset"

# Read input dataset
raw_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load(input_dataset)

# Creating unique id based on the combination of multiple headers
raw_df = raw_df.withColumn("member_id", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))

# Total count of input dataset
raw_df.count()

# Total unique count of member_id
raw_df.filter(col("member_id").isNotNull()).select("member_id").distinct().count()

# Check the count of individual member_id
raw_df \
.groupBy("member_id") \
.count() \
.filter(col("count") > 1) \
.orderBy(col("count").desc()) \
.select(col("member_id"), col("count").alias("total_cnt")) \
.show()

# Creating raw dataset for customer, loan, loan_repayment, loan_defaulter

# customer dataset
raw_df.select(
        "member_id",
        "emp_title",
        "emp_length",
        "home_ownership",
        "annual_inc",
        "addr_state",
        "zip_code",
        lit("USA").alias("country"),
        "grade",
        "sub_grade",
        "verification_status",
        "tot_hi_cred_lim",
        "application_type",
        "annual_inc_joint",
        "verification_status_joint"
    ) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .option("path", "data/data_processing/raw/customer") \
    .save()

# loan dataset
raw_df.select(
        col("id").alias("loan_id"),
        "member_id",
        "loan_amnt",
        "funded_amnt",
        "term",
        "int_rate",
        "installment",
        "issue_d",
        "loan_status",
        "purpose",
        "title"
    ) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .option("path", "data/data_processing/raw/loan") \
    .save()

# loan_repayment dataset
raw_df.select(
        col("id").alias("loan_id"),
        "total_rec_prncp",
        "total_rec_int",
        "total_rec_late_fee",
        "total_pymnt",
        "last_pymnt_amnt",
        "last_pymnt_d",
        "next_pymnt_d"
    ) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .option("path", "data/data_processing/raw/loan_repayment") \
    .save()

# loan_defaulter dataset
raw_df.select(
        col("id").alias("loan_id"),
        "total_rec_prncp",
        "total_rec_int",
        "total_rec_late_fee",
        "total_pymnt",
        "last_pymnt_amnt",
        "last_pymnt_d",
        "next_pymnt_d"
    ) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .option("path", "data/data_processing/raw/loan_defaulter") \
    .save()

# Creating dataframes using raw dataset for customer, loan, loan_repayment, loan_defaulter

# customer dataset
raw_customer_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("data/data_processing/raw/customer")

# loan dataset
raw_loan_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("data/data_processing/raw/loan")

# loan_repayment dataset
raw_loan_repayment_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("data/data_processing/raw/loan_repayment")

# loan_defaulter dataset
raw_loan_defaulter_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("data/data_processing/raw/loan_defaulter")

# Handling datatype of raw dataset

# Creating customer raw dataframe with proper datatype
customer_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("emp_title", StringType(), True),
    StructField("emp_length", StringType(), True),
    StructField("home_ownership", StringType(), True),
    StructField("annual_inc", FloatType(), True),
    StructField("addr_state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("sub_grade", StringType(), True),
    StructField("verification_status", StringType(), True),
    StructField("tot_hi_cred_lim", FloatType(), True),
    StructField("application_type", StringType(), True),
    StructField("annual_inc_joint", FloatType(), True),
    StructField("verification_status_joint", StringType(), True)
])

raw_customer_df = spark.read \
.format("csv") \
.option("header", "true") \
.schema(customer_schema) \
.load("data/data_processing/raw/customer")

# Creating loan raw dataframe with proper datatype
loan_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("loan_amount", FloatType(), True),
    StructField("funded_amount", FloatType(), True),
    StructField("loan_term_months", StringType(), True),
    StructField("interest_rate", FloatType(), True),
    StructField("monthly_installment", FloatType(), True),
    StructField("issue_date", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("loan_purpose", StringType(), True),
    StructField("loan_title", StringType(), True)
])

raw_loan_df = spark.read \
.format("csv") \
.option("header", "true") \
.schema(loan_schema) \
.load("data/data_processing/raw/loan")

# Creating loan_repayment raw dataframe with proper datatype
loan_repayment_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("total_principal_received", FloatType(), True),
    StructField("total_interest_received", FloatType(), True),
    StructField("total_late_fee_received", FloatType(), True),
    StructField("total_payment_received", FloatType(), True),
    StructField("last_payment_amount", FloatType(), True),
    StructField("last_payment_date", StringType(), True),
    StructField("next_payment_date", StringType(), True)
])

raw_loan_repayment_df = spark.read \
.format("csv") \
.option("header", "true") \
.schema(loan_repayment_schema) \
.load("data/data_processing/raw/loan_repayment")

# Creating loan_defaulter raw dataframe with proper datatype
loan_defaulter_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("delinq_2yrs", FloatType(), True),
    StructField("delinq_amnt", FloatType(), True),
    StructField("pub_rec", FloatType(), True),
    StructField("pub_rec_bankruptcies", FloatType(), True),
    StructField("inq_last_6mths", FloatType(), True),
    StructField("total_rec_late_fee", FloatType(), True),
    StructField("mths_since_last_delinq", FloatType(), True),
    StructField("mths_since_last_record", FloatType(), True)
])
 
raw_loan_defaulter_df = spark.read \
.format("csv") \
.option("header", "true") \
.schema(loan_defaulter_schema) \
.load("data/data_processing/raw/loan_defaulter")

# Processing customer dataset

# Rename columns
customer_df_renamed = raw_customer_df.withColumnRenamed("annual_inc", "annual_income") \
.withColumnRenamed("addr_state", "address_state") \
.withColumnRenamed("zip_code", "address_zipcode") \
.withColumnRenamed("country", "address_country") \
.withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
.withColumnRenamed("annual_inc_joint", "join_annual_income")

# Insert a new column named as ingestion date(current time)
customers_df_ingestd = customer_df_renamed.withColumn("ingest_date", current_timestamp())

customers_df_ingestd = customers_df_ingestd.withColumn("ingest_date", col("ingest_date").cast(StringType()))

# Remove duplicates
customers_distinct = customers_df_ingestd.distinct()

# Remove the rows where annual_income is null
customers_income_filtered = customers_distinct.filter(customers_distinct["annual_income"].isNotNull())

# Convert emp_length to integer
customers_emplength_cleaned = customers_income_filtered.withColumn(
    "emp_length", 
    regexp_replace(col("emp_length"), r"(\D)", "")
)

customers_emplength_casted = customers_emplength_cleaned.withColumn("emp_length", customers_emplength_cleaned.emp_length.cast('int'))

# Replace all the nulls in emp_length column with average
avg_emp_length_df = customers_emplength_casted.select(floor(avg("emp_length")).alias("avg_emp_length"))

avg_emp_duration = avg_emp_length_df.collect()[0]["avg_emp_length"]

customers_emplength_replaced = customers_emplength_casted.na.fill(avg_emp_duration, subset=["emp_length"])

# Clean the address_state(it should be 2 characters only),replace all others with NA
customers_state_cleaned = customers_emplength_replaced.withColumn(
    "address_state",
    when(length(col("address_state"))> 2, "NA").otherwise(col("address_state"))
)

customers_state_cleaned.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/customer_parquet") \
.save()

customers_state_cleaned.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/customer_csv") \
.save()

# Processing loan dataset

# Insert a new column named as ingestion date(current time)
loans_df_ingestd = raw_loan_df.withColumn("ingest_date", current_timestamp())

loans_df_ingestd = loans_df_ingestd.withColumn("ingest_date", col("ingest_date").cast(StringType()))

# Dropping the rows which has null values in the mentioned columns
columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]

loans_filtered_df = loans_df_ingestd.na.drop(subset=columns_to_check)

# Convert loan_term_months to integer
loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") \
.cast("int") / 12) \
.cast("int")) \
.withColumnRenamed("loan_term_months","loan_term_years")

# Clean the loans_purpose column
loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]

loans_purpose_modified = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))

loans_purpose_modified.groupBy("loan_purpose").agg(count("*").alias("total")).orderBy(col("total").desc())

loans_purpose_modified.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_parquet") \
.save()

loans_purpose_modified.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_csv") \
.save()

# Processing loan_repayment dataset

# Insert a new column named as ingestion date(current time)
loans_repay_df_ingestd = raw_loan_repayment_df.withColumn("ingest_date", current_timestamp())

loans_repay_df_ingestd = loans_repay_df_ingestd.withColumn("ingest_date", col("ingest_date").cast(StringType()))

# Dropping the rows which has null values in the mentioned columns
columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]

loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset=columns_to_check)

# Update total_payment_received based on criteria
loans_payments_fixed_df = loans_repay_filtered_df.withColumn(
   "total_payment_received",
    when(
        (col("total_principal_received") != 0.0) &
        (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
    ).otherwise(col("total_payment_received"))
)

loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")

# Replace last_payment_date with None if its value is zero; otherwise, keep the existing value
loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
  "last_payment_date",
   when(
       (col("last_payment_date") == 0.0),
       None
       ).otherwise(col("last_payment_date"))
)

# Update last_payment_date to None if next_payment_date is zero; otherwise, set it to next_payment_date
loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
  "last_payment_date",
   when(
       (col("next_payment_date") == 0.0),
       None
       ).otherwise(col("next_payment_date"))
)

loans_payments_ndate_fixed_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_repayment_parquet") \
.save()

loans_payments_ndate_fixed_df.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_repayment_csv") \
.save()

# Processing loan_defaulter dataset

# Convert the delinq_2yrs column to integer type and replace any null values in it with 0
loans_def_processed_df = raw_loan_defaulter_df.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset = ["delinq_2yrs"])

# Filter records where delinq_2yrs or mths_since_last_delinq are greater than 0
loans_def_delinq_df = loans_def_processed_df.filter(
    (col("delinq_2yrs") > 0) | (col("mths_since_last_delinq") > 0)
).select(
    "member_id",
    "delinq_2yrs",
    "delinq_amnt",
    col("mths_since_last_delinq").cast("int").alias("mths_since_last_delinq")  # Cast to int
)

# Filter records with any public records, bankruptcies, or inquiries in the last 6 months, and select member_id
loans_def_records_enq_df = loans_def_processed_df.filter(
    (col("pub_rec") > 0.0) | 
    (col("pub_rec_bankruptcies") > 0.0) | 
    (col("inq_last_6mths") > 0.0)
).select("member_id")

# Convert the pub_rec column to integer type and replace any null values in it with 0
loans_def_p_pub_rec_df = loans_def_processed_df.withColumn("pub_rec", col("pub_rec").cast("integer")).fillna(0, subset = ["pub_rec"])

# Convert the pub_rec_bankruptcies column to integer type and replace any null values in it with 0
loans_def_p_pub_rec_bankruptcies_df = loans_def_p_pub_rec_df.withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset = ["pub_rec_bankruptcies"])

# Convert the inq_last_6mths column to integer type and replace any null values in it with 0
loans_def_p_inq_last_6mths_df = loans_def_p_pub_rec_bankruptcies_df.withColumn("inq_last_6mths", col("inq_last_6mths").cast("integer")).fillna(0, subset = ["inq_last_6mths"])

loans_def_detail_records_enq_df = loans_def_p_inq_last_6mths_df.select(
    "member_id", 
    "pub_rec", 
    "pub_rec_bankruptcies", 
    "inq_last_6mths"
)

loans_def_delinq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_defaulter_deling_parquet") \
.save()

loans_def_delinq_df.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_defaulter_deling_csv") \
.save()

loans_def_records_enq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_defaulter_records_enq_parquet") \
.save()

loans_def_records_enq_df.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_defaulter_records_enq_csv") \
.save()

loans_def_detail_records_enq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_defaulter_detail_records_enq_parquet") \
.save()

loans_def_detail_records_enq_df.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned/loan_defaulter_detail_records_enq_csv") \
.save()

# Creating external tables

# Drop a table if it exist
spark.sql("DROP TABLE IF EXISTS lending_club")

# Create the lending_club database if it does not exist
spark.sql("CREATE DATABASE IF NOT EXISTS lending_club")

# Create the customer external table if it does not exist
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.customer (
    member_id STRING, 
    emp_title STRING, 
    emp_length INT, 
    home_ownership STRING, 
    annual_income FLOAT, 
    address_state STRING, 
    address_zipcode STRING, 
    address_country STRING, 
    grade STRING, 
    sub_grade STRING, 
    verification_status STRING, 
    total_high_credit_limit FLOAT, 
    application_type STRING, 
    join_annual_income FLOAT, 
    verification_status_joint STRING, 
    ingest_date STRING
)
STORED AS PARQUET 
LOCATION 'data/data_processing/cleaned/customer_parquet'
""")

# Create the loan external table if it does not exist
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loan (
        loan_id STRING,
        member_id STRING,
        loan_amount FLOAT,
        funded_amount FLOAT,
        loan_term_years INTEGER,
        interest_rate FLOAT,
        monthly_installment FLOAT,
        issue_date STRING,
        loan_status STRING,
        loan_purpose STRING,
        loan_title STRING,
        ingest_date STRING
    )
    STORED AS PARQUET
    LOCATION 'data/data_processing/cleaned/loan_parquet'
""")

# Create the loan_repayment external table if it does not exist
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loan_repayment (
        loan_id STRING,
        total_principal_received FLOAT,
        total_interest_received FLOAT,
        total_late_fee_received FLOAT,
        total_payment_received FLOAT,
        last_payment_amount FLOAT,
        last_payment_date STRING,
        next_payment_date STRING,
        ingest_date STRING
    )
    STORED AS PARQUET
    LOCATION 'data/data_processing/cleaned/loan_repayment_parquet'
""")

# Create the loan_defaulter_deling external table if it does not exist
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loan_defaulter_deling (
        member_id STRING,
        delinq_2yrs INTEGER,
        delinq_amnt FLOAT,
        mths_since_last_delinq INTEGER
    )
    STORED AS PARQUET
    LOCATION 'data/data_processing/cleaned/loan_defaulter_deling_parquet'
""")

# Create the loan_defaulter_detail_records_enq external table if it does not exist
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loan_defaulter_detail_records_enq (
        member_id STRING,
        pub_rec INTEGER,
        pub_rec_bankruptcies INTEGER,
        inq_last_6mths INTEGER
    )
    STORED AS PARQUET
    LOCATION 'data/data_processing/cleaned/loan_defaulter_detail_records_enq_parquet'
""")

# Creating views by joining tables

# Creating view
spark.sql("""
    CREATE OR REPLACE VIEW lending_club.customers_loan_view AS
    SELECT
        l.loan_id,
        c.member_id,
        c.emp_title,
        c.emp_length,
        c.home_ownership,
        c.annual_income,
        c.address_state,
        c.address_zipcode,
        c.address_country,
        c.grade,
        c.sub_grade,
        c.verification_status,
        c.total_high_credit_limit,
        c.application_type,
        c.join_annual_income,
        c.verification_status_joint,
        l.loan_amount,
        l.funded_amount,
        l.loan_term_years,
        l.interest_rate,
        l.monthly_installment,
        l.issue_date,
        l.loan_status,
        l.loan_purpose,
        r.total_principal_received,
        r.total_interest_received,
        r.total_late_fee_received,
        r.last_payment_date,
        r.next_payment_date,
        d.delinq_2yrs,
        d.delinq_amnt,
        d.mths_since_last_delinq,
        e.pub_rec,
        e.pub_rec_bankruptcies,
        e.inq_last_6mths
    FROM lending_club.customer c
    LEFT JOIN lending_club.loan l ON c.member_id = l.member_id
    LEFT JOIN lending_club.loan_repayment r ON l.loan_id = r.loan_id
    LEFT JOIN lending_club.loan_defaulter_deling d ON c.member_id = d.member_id
    LEFT JOIN lending_club.loan_defaulter_detail_records_enq e ON c.member_id = e.member_id
""")

# Creating table
spark.sql("""
    CREATE TABLE lending_club.customers_loan AS
    SELECT
        l.loan_id,
        c.member_id,
        c.emp_title,
        c.emp_length,
        c.home_ownership,
        c.annual_income,
        c.address_state,
        c.address_zipcode,
        c.address_country,
        c.grade,
        c.sub_grade,
        c.verification_status,
        c.total_high_credit_limit,
        c.application_type,
        c.join_annual_income,
        c.verification_status_joint,
        l.loan_amount,
        l.funded_amount,
        l.loan_term_years,
        l.interest_rate,
        l.monthly_installment,
        l.issue_date,
        l.loan_status,
        l.loan_purpose,
        r.total_principal_received,
        r.total_interest_received,
        r.total_late_fee_received,
        r.last_payment_date,
        r.next_payment_date,
        d.delinq_2yrs,
        d.delinq_amnt,
        d.mths_since_last_delinq,
        e.pub_rec,
        e.pub_rec_bankruptcies,
        e.inq_last_6mths
    FROM lending_club.customer c
    LEFT JOIN lending_club.loan l ON c.member_id = l.member_id
    LEFT JOIN lending_club.loan_repayment r ON l.loan_id = r.loan_id
    LEFT JOIN lending_club.loan_defaulter_deling d ON c.member_id = d.member_id
    LEFT JOIN lending_club.loan_defaulter_detail_records_enq e ON c.member_id = e.member_id
""")

# Segregation of the data

# Query to find duplicate member_id in the customer table
bad_data_customer_df = spark.sql("""
    SELECT member_id 
    FROM (
        SELECT member_id, COUNT(*) AS total 
        FROM lending_club.customer 
        GROUP BY member_id 
        HAVING total > 1
    )
""")

# Query to find duplicate member_id in the loan_defaulter_deling table
bad_data_loans_defaulters_delinq_df = spark.sql("""
    SELECT member_id 
    FROM (
        SELECT member_id, COUNT(*) AS total 
        FROM lending_club.loan_defaulter_deling 
        GROUP BY member_id 
        HAVING total > 1
    )
""")

# Query to find duplicate member_id in the loan_defaulter_detail_records_enq table
bad_data_loans_defaulters_detail_rec_enq_df = spark.sql("""
    SELECT member_id 
    FROM (
        SELECT member_id, COUNT(*) AS total 
        FROM lending_club.loan_defaulter_detail_records_enq 
        GROUP BY member_id 
        HAVING total > 1
    )
""")

bad_data_customer_df.write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "data/data_processing/bad/bad_data_customer") \
.save()

bad_data_loans_defaulters_delinq_df.write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "data/data_processing/bad/bad_data_loan_defaulter_deling") \
.save()

bad_data_loans_defaulters_detail_rec_enq_df.write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "data/data_processing/bad/bad_data_loan_defaulter_detail_records_enq") \
.save()

# Combine bad customer data
bad_customer_data_df = bad_data_customer_df.select("member_id") \
    .union(bad_data_loans_defaulters_delinq_df.select("member_id")) \
    .union(bad_data_loans_defaulters_detail_rec_enq_df.select("member_id"))

# Remove duplicate
bad_customer_data_final_df = bad_customer_data_df.distinct()

bad_customer_data_final_df.repartition(1).write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "data/data_processing/bad/bad_customer_data_final") \
.save()

# Create or replace a temporary view for the final bad customer data
bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")

# Query to retrieve customers excluding those in the bad_data_customer view
customers_df = spark.sql("""
    SELECT * 
    FROM lending_club.customer 
    WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
""")

# Query to retrieve loan defaulters excluding those in the bad_data_customer view
loans_defaulters_delinq_df = spark.sql("""
    SELECT * 
    FROM lending_club.loan_defaulter_deling 
    WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
""")

# Query to retrieve loan defaulters detail records excluding those in the bad_data_customer view
loans_defaulters_detail_rec_enq_df = spark.sql("""
    SELECT * 
    FROM lending_club.loan_defaulter_detail_records_enq 
    WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
""")

customers_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned_final/customer_parquet") \
.save()

loans_defaulters_delinq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned_final/loan_defaulter_deling_parquet") \
.save()

loans_defaulters_detail_rec_enq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/cleaned_final/loan_defaulter_detail_records_enq_parquet") \
.save()

# Create an external table for the cleaned customer data
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.customer_new (
        member_id STRING,
        emp_title STRING,
        emp_length INT,
        home_ownership STRING,
        annual_income FLOAT,
        address_state STRING,
        address_zipcode STRING,
        address_country STRING,
        grade STRING,
        sub_grade STRING,
        verification_status STRING,
        total_high_credit_limit FLOAT,
        application_type STRING,
        join_annual_income FLOAT,
        verification_status_joint STRING,
        ingest_date STRING
    )
    STORED AS PARQUET
    LOCATION 'data/data_processing/cleaned_final/customer_parquet'
""")

# Create an external table for the cleaned loan defaulters data
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loan_defaulter_deling_new (
        member_id STRING,
        delinq_2yrs INTEGER,
        delinq_amnt FLOAT,
        mths_since_last_delinq INTEGER
    )
    STORED AS PARQUET
    LOCATION 'data/data_processing/cleaned_final/loan_defaulter_deling_parquet'
""")

# Create an external table for the cleaned loan defaulters detail records data
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loan_defaulter_detail_records_enq_new (
        member_id STRING,
        pub_rec INTEGER,
        pub_rec_bankruptcies INTEGER,
        inq_last_6mths INTEGER
    )
    STORED AS PARQUET
    LOCATION 'data/data_processing/cleaned_final/loan_defaulter_detail_records_enq_parquet'
""")

# Calculating Loan Score

# Associating points to the grades in order to calculate the loan core
spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
spark.conf.set("spark.sql.very_bad_rated_pts", 100)
spark.conf.set("spark.sql.bad_rated_pts", 250)
spark.conf.set("spark.sql.good_rated_pts", 500)
spark.conf.set("spark.sql.very_good_rated_pts", 650)
spark.conf.set("spark.sql.excellent_rated_pts", 800)

spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
spark.conf.set("spark.sql.bad_grade_pts", 1500)
spark.conf.set("spark.sql.good_grade_pts", 2000)
spark.conf.set("spark.sql.very_good_grade_pts", 2500)

# Criteria 1: Payment History

bad_customer_data_final_df = spark.read \
.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.load("data/data_processing/bad/bad_customer_data_final")

# Create or replace a temporary view for the final bad customer data
bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")

# Calculate payment points based on last payment amount and total payment received for each member
ph_df = spark.sql("""
    SELECT c.member_id,
        CASE 
            WHEN p.last_payment_amount < (c.monthly_installment * 0.5) THEN ${spark.sql.very_bad_rated_pts}
            WHEN p.last_payment_amount >= (c.monthly_installment * 0.5) AND p.last_payment_amount < c.monthly_installment THEN ${spark.sql.very_bad_rated_pts}
            WHEN p.last_payment_amount = c.monthly_installment THEN ${spark.sql.good_rated_pts}
            WHEN p.last_payment_amount > c.monthly_installment AND p.last_payment_amount <= (c.monthly_installment * 1.50) THEN ${spark.sql.very_good_rated_pts}
            WHEN p.last_payment_amount > (c.monthly_installment * 1.50) THEN ${spark.sql.excellent_rated_pts}
            ELSE ${spark.sql.unacceptable_rated_pts}
        END AS last_payment_pts,
        CASE 
            WHEN p.total_payment_received >= (c.funded_amount * 0.50) THEN ${spark.sql.very_good_rated_pts}
            WHEN p.total_payment_received < (c.funded_amount * 0.50) AND p.total_payment_received > 0 THEN ${spark.sql.good_rated_pts}
            WHEN p.total_payment_received = 0 OR p.total_payment_received IS NULL THEN ${spark.sql.unacceptable_rated_pts}
        END AS total_payment_pts
    FROM lending_club.loan_repayment p
    INNER JOIN lending_club.loan c ON c.loan_id = p.loan_id
    WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
""")

# Criteria 2: Loan Defaulters History

# Create or replace a temporary view for ph_df
ph_df.createOrReplaceTempView("ph_pts")

# Calculate points based on delinquency and public record metrics for loan defaulters
ldh_ph_df = spark.sql(
    """
    SELECT p.*, 
        CASE 
            WHEN d.delinq_2yrs = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN d.delinq_2yrs BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN d.delinq_2yrs BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN d.delinq_2yrs > 5 OR d.delinq_2yrs IS NULL THEN ${spark.sql.unacceptable_grade_pts} 
        END AS delinq_pts, 
        CASE 
            WHEN l.pub_rec = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.pub_rec BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN l.pub_rec BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.pub_rec > 5 OR l.pub_rec IS NULL THEN ${spark.sql.very_bad_rated_pts} 
        END AS public_records_pts, 
        CASE 
            WHEN l.pub_rec_bankruptcies = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.pub_rec_bankruptcies BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN l.pub_rec_bankruptcies BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.pub_rec_bankruptcies > 5 OR l.pub_rec_bankruptcies IS NULL THEN ${spark.sql.very_bad_rated_pts} 
        END AS public_bankruptcies_pts, 
        CASE 
            WHEN l.inq_last_6mths = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.inq_last_6mths BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN l.inq_last_6mths BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.inq_last_6mths > 5 OR l.inq_last_6mths IS NULL THEN ${spark.sql.unacceptable_rated_pts} 
        END AS enq_pts 
    FROM lending_club.loan_defaulter_detail_records_enq_new l 
    INNER JOIN lending_club.loan_defaulter_deling_new d ON d.member_id = l.member_id  
    INNER JOIN ph_pts p ON p.member_id = l.member_id 
    WHERE l.member_id NOT IN (SELECT member_id FROM bad_data_customer)
    """
)

# Create or replace a temporary view for ldh_ph_df
ldh_ph_df.createOrReplaceTempView("ldh_ph_pts")

# Calculate loan and customer scores based on various criteria
fh_ldh_ph_df = spark.sql(
    """
    SELECT ldef.*, 
        CASE 
            WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN ${spark.sql.excellent_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%current%' THEN ${spark.sql.good_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN ${spark.sql.bad_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN ${spark.sql.very_bad_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN ${spark.sql.unacceptable_rated_pts} 
            ELSE ${spark.sql.unacceptable_rated_pts} 
        END AS loan_status_pts, 
        CASE 
            WHEN LOWER(a.home_ownership) LIKE '%own' THEN ${spark.sql.excellent_rated_pts} 
            WHEN LOWER(a.home_ownership) LIKE '%rent' THEN ${spark.sql.good_rated_pts} 
            WHEN LOWER(a.home_ownership) LIKE '%mortgage' THEN ${spark.sql.bad_rated_pts} 
            WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL THEN ${spark.sql.very_bad_rated_pts} 
        END AS home_pts, 
        CASE 
            WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20) THEN ${spark.sql.very_good_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30) THEN ${spark.sql.good_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50) THEN ${spark.sql.bad_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70) THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) THEN ${spark.sql.unacceptable_rated_pts} 
            ELSE ${spark.sql.unacceptable_rated_pts} 
        END AS credit_limit_pts, 
        CASE 
            WHEN (a.grade) = 'A' AND (a.sub_grade)='A1' THEN ${spark.sql.excellent_rated_pts} 
            WHEN (a.grade) = 'A' AND (a.sub_grade)='A2' THEN (${spark.sql.excellent_rated_pts} * 0.95) 
            WHEN (a.grade) = 'A' AND (a.sub_grade)='A3' THEN (${spark.sql.excellent_rated_pts} * 0.90) 
            WHEN (a.grade) = 'A' AND (a.sub_grade)='A4' THEN (${spark.sql.excellent_rated_pts} * 0.85) 
            WHEN (a.grade) = 'A' AND (a.sub_grade)='A5' THEN (${spark.sql.excellent_rated_pts} * 0.80) 
            WHEN (a.grade) = 'B' AND (a.sub_grade)='B1' THEN ${spark.sql.very_good_rated_pts} 
            WHEN (a.grade) = 'B' AND (a.sub_grade)='B2' THEN (${spark.sql.very_good_rated_pts} * 0.95) 
            WHEN (a.grade) = 'B' AND (a.sub_grade)='B3' THEN (${spark.sql.very_good_rated_pts} * 0.90) 
            WHEN (a.grade) = 'B' AND (a.sub_grade)='B4' THEN (${spark.sql.very_good_rated_pts} * 0.85) 
            WHEN (a.grade) = 'B' AND (a.sub_grade)='B5' THEN (${spark.sql.very_good_rated_pts} * 0.80) 
            WHEN (a.grade) = 'C' AND (a.sub_grade)='C1' THEN ${spark.sql.good_rated_pts} 
            WHEN (a.grade) = 'C' AND (a.sub_grade)='C2' THEN (${spark.sql.good_rated_pts} * 0.95) 
            WHEN (a.grade) = 'C' AND (a.sub_grade)='C3' THEN (${spark.sql.good_rated_pts} * 0.90) 
            WHEN (a.grade) = 'C' AND (a.sub_grade)='C4' THEN (${spark.sql.good_rated_pts} * 0.85) 
            WHEN (a.grade) = 'C' AND (a.sub_grade)='C5' THEN (${spark.sql.good_rated_pts} * 0.80) 
            WHEN (a.grade) = 'D' AND (a.sub_grade)='D1' THEN ${spark.sql.bad_rated_pts} 
            WHEN (a.grade) = 'D' AND (a.sub_grade)='D2' THEN (${spark.sql.bad_rated_pts} * 0.95) 
            WHEN (a.grade) = 'D' AND (a.sub_grade)='D3' THEN (${spark.sql.bad_rated_pts} * 0.90) 
            WHEN (a.grade) = 'D' AND (a.sub_grade)='D4' THEN (${spark.sql.bad_rated_pts} * 0.85) 
            WHEN (a.grade) = 'D' AND (a.sub_grade)='D5' THEN (${spark.sql.bad_rated_pts} * 0.80) 
            WHEN (a.grade) = 'E' AND (a.sub_grade)='E1' THEN (${spark.sql.very_bad_rated_pts}) 
            WHEN (a.grade) = 'E' AND (a.sub_grade)='E2' THEN (${spark.sql.very_bad_rated_pts} * 0.95) 
            WHEN (a.grade) = 'E' AND (a.sub_grade)='E3' THEN (${spark.sql.very_bad_rated_pts} * 0.90) 
            WHEN (a.grade) = 'E' AND (a.sub_grade)='E4' THEN (${spark.sql.very_bad_rated_pts} * 0.85) 
            WHEN (a.grade) = 'E' AND (a.sub_grade)='E5' THEN (${spark.sql.very_bad_rated_pts} * 0.80) 
            WHEN (a.grade) IN ('F', 'G') THEN (${spark.sql.unacceptable_rated_pts}) 
        END AS grade_pts 
    FROM ldh_ph_pts ldef 
    INNER JOIN lending_club.loan l ON ldef.member_id = l.member_id 
    INNER JOIN lending_club.customer_new a ON a.member_id = ldef.member_id 
    WHERE ldef.member_id NOT IN (SELECT member_id FROM bad_data_customer)
    """
)

# Create a temporary view for the DataFrame
fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")

# Final Loan Score calculation based on below criteria
# 1) Payment History = 20%
# 2) Loan Defaults = 45%
# 3) Financial Health = 35%

loan_score = spark.sql("""
    SELECT member_id, 
    ((last_payment_pts + total_payment_pts) * 0.20) AS payment_history_pts, 
    ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) AS defaulters_history_pts, 
    ((loan_status_pts + home_pts + credit_limit_pts + grade_pts) * 0.35) AS financial_health_pts 
    FROM fh_ldh_ph_pts
""")

# Calculate the final loan score by summing up various points
final_loan_score = loan_score.withColumn(
    'loan_score',
    loan_score.payment_history_pts + 
    loan_score.defaulters_history_pts + 
    loan_score.financial_health_pts
)

# Create a temporary view for loan score evaluation
final_loan_score.createOrReplaceTempView("loan_score_eval")

# Assign final grades based on loan scores using SQL query
loan_score_final = spark.sql("""
    SELECT ls.*, 
    CASE 
        WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN 'A' 
        WHEN loan_score <= ${spark.sql.very_good_grade_pts} AND loan_score > ${spark.sql.good_grade_pts} THEN 'B' 
        WHEN loan_score <= ${spark.sql.good_grade_pts} AND loan_score > ${spark.sql.bad_grade_pts} THEN 'C' 
        WHEN loan_score <= ${spark.sql.bad_grade_pts} AND loan_score > ${spark.sql.very_bad_grade_pts} THEN 'D' 
        WHEN loan_score <= ${spark.sql.very_bad_grade_pts} AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN 'E'  
        WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN 'F' 
    END AS loan_final_grade 
    FROM loan_score_eval ls
""")

loan_score_final.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "data/data_processing/processed/loan_score") \
.save()

spark.stop()