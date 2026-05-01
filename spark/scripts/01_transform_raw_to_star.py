from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("raw-to-star") \
    .config("spark.sql.session.timeZone", "UTC").getOrCreate()

PG_URL = "jdbc:postgresql://lab2postgres:5432/postgreslab2"
PG_PROPS = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

print("Чтение mock_data...")
df = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)

dim_customer = df.select(
    "customer_first_name", "customer_last_name", "customer_age",
    "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed"
).dropDuplicates(["customer_email"])

dim_seller = df.select(
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code"
).dropDuplicates(["seller_email"])

dim_store = df.select(
    "store_name", "store_location", "store_city", "store_state",
    "store_country", "store_phone", "store_email"
).dropDuplicates(["store_email"])

dim_supplier = df.select(
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
    "supplier_address", "supplier_city", "supplier_country"
).dropDuplicates(["supplier_email"])

dim_product = df.select(
    "product_name", "product_category", "pet_category", "product_price",
    "product_quantity", "product_weight", "product_color", "product_size",
    "product_brand", "product_material", "product_description", "product_rating",
    "product_reviews", "product_release_date", "product_expiry_date"
).dropDuplicates(["product_name", "product_category", "product_brand"])

tables = [
    ("dim_customer", dim_customer), ("dim_seller", dim_seller), ("dim_store", dim_store),
    ("dim_supplier", dim_supplier), ("dim_product", dim_product)
]

for name, table in tables:
    print(f"-------------------------> {name} <-------------------------")
    table.write.mode("overwrite").option("truncate", "true").option("cascadeTruncate", "true").jdbc(PG_URL, name, properties=PG_PROPS)

dim_customer_db = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
dim_seller_db = spark.read.jdbc(PG_URL, "dim_seller", properties=PG_PROPS)
dim_store_db = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
dim_supplier_db = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
dim_product_db = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)

print("Сборка fact_sales...")

fact = df \
    .join(dim_customer_db.select("customer_id", "customer_email"), "customer_email") \
    .join(dim_seller_db.select("seller_id", "seller_email"), "seller_email") \
    .join(dim_store_db.select("store_id", "store_email"), "store_email") \
    .join(dim_supplier_db.select("supplier_id", "supplier_email"), "supplier_email") \
    .join(
        dim_product_db.select("product_id", "product_name", "product_category", "product_brand"),
        ["product_name", "product_category", "product_brand"]
    )

fact_sales = fact.select(
    F.col("id").alias("mock_data_id"), "sale_date", "customer_id", "seller_id",
    "store_id", "supplier_id", "product_id", "sale_quantity", "sale_total_price"
)

fact_sales.write.mode("overwrite").option("truncate", "true") \
    .jdbc(PG_URL, "fact_sales", properties=PG_PROPS)

spark.stop()
