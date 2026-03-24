# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: Generate Customer Data
# MAGIC
# MAGIC This notebook creates a sample CSV file with 20 customer records (including one intentional
# MAGIC duplicate for deduplication demonstration) and writes it to the DBFS landing zone.
# MAGIC The file will be picked up by the AutoLoader in the next notebook.

# COMMAND ----------

# Define the landing zone path where raw CSV files will be placed for AutoLoader ingestion
LANDING_ZONE = "dbfs:/etl_demo/landing/customers/"

# COMMAND ----------

# Customer data: 19 unique records + 1 duplicate of customer_id 1002 (Bob Smith)
customer_data = """customer_id,first_name,last_name,email,phone,address,city,state,zip_code,country,age,gender,signup_date,loyalty_tier,total_purchases
1001,Alice,Johnson,alice.johnson@email.com,555-101-2001,123 Maple St,Austin,TX,78701,USA,34,F,2021-03-15,Gold,1250.75
1002,Bob,Smith,bob.smith@email.com,555-102-2002,456 Oak Ave,Seattle,WA,98101,USA,45,M,2020-07-22,Platinum,4800.00
1003,Carol,Williams,carol.williams@email.com,555-103-2003,789 Pine Rd,Chicago,IL,60601,USA,29,F,2022-01-10,Silver,620.50
1004,David,Brown,david.brown@email.com,555-104-2004,321 Elm St,New York,NY,10001,USA,52,M,2019-11-05,Platinum,9200.00
1005,Eve,Davis,eve.davis@email.com,555-105-2005,654 Cedar Blvd,Los Angeles,CA,90001,USA,38,F,2021-06-18,Gold,2100.25
1006,Frank,Miller,frank.miller@email.com,555-106-2006,987 Birch Ln,Houston,TX,77001,USA,41,M,2020-03-30,Silver,890.00
1007,Grace,Wilson,grace.wilson@email.com,555-107-2007,111 Walnut Dr,Phoenix,AZ,85001,USA,27,F,2022-05-14,Bronze,310.00
1008,Henry,Moore,henry.moore@email.com,555-108-2008,222 Spruce Ct,Philadelphia,PA,19101,USA,36,M,2021-09-25,Gold,1750.50
1009,Iris,Taylor,iris.taylor@email.com,555-109-2009,333 Ash Way,San Antonio,TX,78201,USA,31,F,2020-12-01,Silver,540.75
1010,Jack,Anderson,jack.anderson@email.com,555-110-2010,444 Poplar Ave,San Diego,CA,92101,USA,48,M,2019-08-17,Platinum,6500.00
1011,Karen,Thomas,karen.thomas@email.com,555-111-2011,555 Hickory St,Dallas,TX,75201,USA,33,F,2021-02-28,Gold,1980.00
1012,Leo,Jackson,leo.jackson@email.com,555-112-2012,666 Magnolia Blvd,San Jose,CA,95101,USA,25,M,2022-07-04,Bronze,200.50
1013,Mia,White,mia.white@email.com,555-113-2013,777 Chestnut Rd,Austin,TX,78702,USA,44,F,2020-05-19,Gold,3300.25
1014,Noah,Harris,noah.harris@email.com,555-114-2014,888 Cypress Ave,Jacksonville,FL,32099,USA,39,M,2021-10-11,Silver,720.00
1015,Olivia,Martin,olivia.martin@email.com,555-115-2015,999 Willow Ln,Columbus,OH,43004,USA,26,F,2022-03-22,Bronze,150.00
1016,Paul,Garcia,paul.garcia@email.com,555-116-2016,101 Sycamore St,Charlotte,NC,28201,USA,55,M,2019-04-09,Platinum,11500.00
1017,Quinn,Martinez,quinn.martinez@email.com,555-117-2017,202 Juniper Dr,Indianapolis,IN,46201,USA,30,F,2021-08-16,Silver,430.75
1018,Ryan,Robinson,ryan.robinson@email.com,555-118-2018,303 Sequoia Ct,San Francisco,CA,94101,USA,43,M,2020-01-27,Gold,2800.00
1019,Sara,Clark,sara.clark@email.com,555-119-2019,404 Redwood Way,Denver,CO,80201,USA,35,F,2021-12-05,Silver,960.50
1002,Bob,Smith,bob.smith@email.com,555-102-2002,456 Oak Ave,Seattle,WA,98101,USA,45,M,2020-07-22,Platinum,4800.00"""

# COMMAND ----------

# Write the CSV content to DBFS so AutoLoader can pick it up
dbutils.fs.mkdirs(LANDING_ZONE)

# Write raw CSV string to a file in the landing zone
dbutils.fs.put(
    LANDING_ZONE + "customers_20240101.csv",
    customer_data,
    overwrite=True
)

print(f"Customer CSV written to: {LANDING_ZONE}")

# COMMAND ----------

# Verify the file was written correctly
files = dbutils.fs.ls(LANDING_ZONE)
for f in files:
    print(f"File: {f.name}  |  Size: {f.size} bytes")

# COMMAND ----------

# Preview first few lines of the written file
content = dbutils.fs.head(LANDING_ZONE + "customers_20240101.csv", 500)
print(content)
