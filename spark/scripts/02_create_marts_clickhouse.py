from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import urllib.request
import base64

spark = SparkSession.builder.appName("ETLClickHouseMarts") \
    .config("spark.sql.session.timeZone", "UTC").getOrCreate()

PG_URL = "jdbc:postgresql://lab2postgres:5432/postgreslab2"
PG_PROPS = { "user": "user", "password": "password", "driver": "org.postgresql.Driver" }

CH_DB = "clickhouselab2"
CH_URL = f"jdbc:clickhouse://lab2clickhouse:8123/{CH_DB}"
CH_PROPS = { "user": "user", "password": "password","driver": "com.clickhouse.jdbc.ClickHouseDriver" }
CH_base_url = "http://lab2clickhouse:8123"

fact = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
dim_product = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
dim_customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
dim_store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
dim_supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)

base = fact.join(dim_product, "product_id").join(dim_customer, "customer_id") \
    .join(dim_store, "store_id").join(dim_supplier, "supplier_id")


w_prod_qty = Window.orderBy(F.col("total_qty_sold").desc())
w_cat = Window.partitionBy("product_category")
marts_product = base.groupBy("product_id", "product_name", "product_category", "product_brand"
).agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.sum("sale_quantity").alias("total_qty_sold"),
    F.avg("product_rating").alias("avg_rating"),
    F.sum("product_reviews").alias("total_reviews")
).withColumn("category_total_revenue", F.sum("total_revenue").over(w_cat)
).withColumn("is_top10_sold", (F.rank().over(w_prod_qty) <= 10).cast("int"))


w_cust = Window.orderBy(F.col("total_spend").desc())
w_country = Window.partitionBy("customer_country")
marts_customer = base.groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country"
).agg(F.sum("sale_total_price").alias("total_spend"), F.count("sale_id").alias("order_count")
).withColumn("avg_check", F.col("total_spend") / F.col("order_count")
).withColumn("country_total_spend", F.sum("total_spend").over(w_country)
).withColumn("is_top10_spend", (F.rank().over(w_cust) <= 10).cast("int"))


base_time = base.withColumn("year", F.year("sale_date")).withColumn("month", F.month("sale_date"))
w_time = Window.orderBy("year", "month")
marts_time = base_time.groupBy("year", "month").agg(
    F.sum("sale_total_price").alias("monthly_revenue"),
    F.sum("sale_quantity").alias("total_qty"), F.count("sale_id").alias("order_count")
).withColumn("avg_order_size", F.col("monthly_revenue") / F.col("order_count")
).withColumn("prev_month_revenue", F.lag("monthly_revenue").over(w_time)
)


w_store = Window.orderBy(F.col("total_revenue").desc())
w_loc = Window.partitionBy("store_city", "store_country")
marts_store = base.groupBy("store_id", "store_name", "store_city", "store_country"
).agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("sale_id").alias("order_count")
).withColumn("avg_check", F.col("total_revenue") / F.col("order_count")
).withColumn("location_total_sales", F.sum("order_count").over(w_loc)
).withColumn("is_top5_revenue", (F.rank().over(w_store) <= 5).cast("int")
)


w_sup = Window.orderBy(F.col("total_revenue").desc())
w_sup_country = Window.partitionBy("supplier_country")
marts_supplier = base.groupBy("supplier_id", "supplier_name", "supplier_country"
).agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.avg("product_price").alias("avg_product_price"),
    F.sum("sale_quantity").alias("total_qty_sold")
).withColumn("country_total_revenue", F.sum("total_revenue").over(w_sup_country)
).withColumn("is_top5_revenue", (F.rank().over(w_sup) <= 5).cast("int")
)

prod_quality = base.groupBy("product_id", "product_name", "product_rating"
).agg(
    F.sum("sale_total_price").alias("total_revenue"), F.sum("product_reviews").alias("total_reviews")
)

prod_quality = base.groupBy("product_id", "product_name", "product_rating").agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.sum("product_reviews").alias("total_reviews"),
    F.sum("sale_quantity").alias("total_qty_sold")
)

corr_rating_revenue = prod_quality.stat.corr("product_rating", "total_revenue")
corr_rating_qty = prod_quality.stat.corr("product_rating", "total_qty_sold")

w_global = Window.partitionBy()

marts_quality = prod_quality \
    .withColumn("max_rating", F.max("product_rating").over(w_global)) \
    .withColumn("min_rating", F.min("product_rating").over(w_global)) \
    .withColumn("max_reviews", F.max("total_reviews").over(w_global)) \
    .withColumn("is_highest_rating", (F.col("product_rating") == F.col("max_rating")).cast("int")) \
    .withColumn("is_lowest_rating", (F.col("product_rating") == F.col("min_rating")).cast("int")) \
    .withColumn("is_most_reviews", (F.col("total_reviews") == F.col("max_reviews")).cast("int")) \
    .withColumn("rating_revenue_correlation", F.lit(round(corr_rating_revenue, 4) if corr_rating_revenue else None)) \
    .withColumn("rating_qty_correlation", F.lit(round(corr_rating_qty, 4) if corr_rating_qty else None)) \
    .drop("max_rating", "min_rating", "max_reviews") \
    .select(
        "product_id", "product_name", "product_rating", "total_reviews", "total_revenue", "total_qty_sold",
        "is_highest_rating", "is_lowest_rating", "is_most_reviews", "rating_revenue_correlation", "rating_qty_correlation"
    )

def create_tables():
    ddl = [
        """CREATE TABLE IF NOT EXISTS clickhouselab2.marts_product (product_id UInt64,
            product_name String, product_category String, product_brand String,
            total_revenue Float64, total_qty_sold UInt64, avg_rating Float32,
            total_reviews UInt64, category_total_revenue Float64, is_top10_sold UInt8
        ) ENGINE = MergeTree() ORDER BY product_id""",

        """CREATE TABLE IF NOT EXISTS clickhouselab2.marts_customer (
            customer_id UInt64, customer_first_name String,
            customer_last_name String, customer_country String,
            total_spend Float64, order_count UInt64, avg_check Float64,
            country_total_spend Float64, is_top10_spend UInt8
        ) ENGINE = MergeTree() ORDER BY customer_id""",

        """CREATE TABLE IF NOT EXISTS clickhouselab2.marts_time (
            year UInt16, month UInt8, monthly_revenue Float64,
            total_qty UInt64, order_count UInt64, avg_order_size Float64,
            prev_month_revenue Nullable(Float64)
        ) ENGINE = MergeTree() ORDER BY (year, month)""",

        """CREATE TABLE IF NOT EXISTS clickhouselab2.marts_store (
            store_id UInt64, store_name String, store_city String,
            store_country String, total_revenue Float64, order_count UInt64,
            avg_check Float64, location_total_sales UInt64, is_top5_revenue UInt8
        ) ENGINE = MergeTree() ORDER BY store_id""",

        """CREATE TABLE IF NOT EXISTS clickhouselab2.marts_supplier (
            supplier_id UInt64, supplier_name String, supplier_country String,
            total_revenue Float64, avg_product_price Float64, total_qty_sold UInt64,
            country_total_revenue Float64, is_top5_revenue UInt8
        ) ENGINE = MergeTree() ORDER BY supplier_id""",

        """CREATE TABLE IF NOT EXISTS clickhouselab2.marts_quality (
            product_id UInt64,
            product_name String,
            product_rating Float32,
            total_reviews UInt64,
            total_revenue Float64,
            total_qty_sold UInt64,
            is_highest_rating UInt8,
            is_lowest_rating UInt8,
            is_most_reviews UInt8,
            rating_revenue_correlation Nullable(Float32),
            rating_qty_correlation Nullable(Float32)
        ) ENGINE = MergeTree() ORDER BY product_id"""
    ]

    for i, q in enumerate(ddl, 1):
        credentials = f"{CH_PROPS['user']}:{CH_PROPS['password']}"
        req = urllib.request.Request(
            f"{CH_base_url}/?database={CH_DB}", data=q.encode('utf-8'),
            headers={
                "Authorization": f"Basic {base64.b64encode(credentials.encode()).decode()}",
                "Content-Type": "text/plain; charset=utf-8"
            }, method="POST"
        )
        urllib.request.urlopen(req, timeout=30)
        print(f"Table {i}/{len(ddl)} created")


def write(df, table):
    df.write.jdbc(url=CH_URL, table=f"{CH_DB}.{table}", properties=CH_PROPS, mode="append")

create_tables()

write(marts_product, "marts_product")
write(marts_customer, "marts_customer")
write(marts_time, "marts_time")
write(marts_store, "marts_store")
write(marts_supplier, "marts_supplier")
write(marts_quality, "marts_quality")

spark.stop()
