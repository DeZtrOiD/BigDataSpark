from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import urllib.request
import base64
import math

spark = (
    SparkSession.builder.appName("ETLClickHouseMarts")
    .config("spark.sql.session.timeZone", "UTC").getOrCreate()
)

PG_URL = "jdbc:postgresql://lab2postgres:5432/postgreslab2"
PG_PROPS = {"user": "user","password": "password","driver": "org.postgresql.Driver",}

CH_DB = "clickhouselab2"
CH_URL = f"jdbc:clickhouse://lab2clickhouse:8123/{CH_DB}"
CH_PROPS = {"user": "user", "password": "password","driver": "com.clickhouse.jdbc.ClickHouseDriver",}
CH_BASE_URL = "http://lab2clickhouse:8123"


fact = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
dim_product = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
dim_customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
dim_store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
dim_supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)

base = (
    fact.join(dim_product, "product_id").join(dim_customer, "customer_id")
        .join(dim_store, "store_id").join(dim_supplier, "supplier_id")
)


w_prod_qty = Window.orderBy(F.col("total_qty_sold").desc())
w_cat = Window.partitionBy("product_category")

marts_product = (
    base.groupBy("product_id", "product_name", "product_category", "product_brand")
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.sum("sale_quantity").alias("total_qty_sold"),
        F.avg("product_rating").alias("avg_rating"),
        F.sum("product_reviews").alias("total_reviews"),
    )
    .withColumn("category_total_revenue", F.sum("total_revenue").over(w_cat))
    .withColumn("category_total_qty_sold", F.sum("total_qty_sold").over(w_cat))
    .withColumn("is_top10_sold", (F.row_number().over(w_prod_qty) <= 10).cast("int"))
)


w_cust = Window.orderBy(F.col("total_spend").desc())
w_country = Window.partitionBy("customer_country")

marts_customer = (
    base.groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country")
    .agg(F.sum("sale_total_price").alias("total_spend"), F.count("sale_id").alias("order_count"))
    .withColumn("avg_check", F.col("total_spend") / F.col("order_count"))
    .withColumn("country_total_spend", F.sum("total_spend").over(w_country))
    .withColumn("country_customer_count", F.count("customer_id").over(w_country))
    .withColumn("is_top10_spend", (F.row_number().over(w_cust) <= 10).cast("int"))
)


base_time = (
    base.withColumn("year", F.year("sale_date").cast("int"))
        .withColumn("month", F.month("sale_date").cast("int"))
        .withColumn("month_name", F.date_format("sale_date", "MMMM"))
)

monthly_time = (
    base_time.groupBy("year", "month", "month_name")
    .agg(
        F.sum("sale_total_price").alias("monthly_revenue"),
        F.sum("sale_quantity").alias("total_qty"),
        F.count("sale_id").alias("order_count"),
    )
)

w_time = Window.orderBy("year_month_key")
w_same_month = Window.partitionBy("month").orderBy("year")

marts_time = (
    monthly_time
    .withColumn("year_month_key", F.col("year") * 100 + F.col("month"))
    .withColumn(
        "yearly_revenue", F.sum("monthly_revenue").over(Window.partitionBy("year"))
    )
    .withColumn("avg_order_size", F.col("monthly_revenue") / F.col("order_count"))
    .withColumn("prev_month_revenue", F.lag("monthly_revenue").over(w_time))
    .withColumn(
        "month_over_month_growth_pct",
        F.when(
            F.col("prev_month_revenue").isNull() | (F.col("prev_month_revenue") == 0), F.lit(None)
        ).otherwise(
            (F.col("monthly_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue") * 100
        )
    )
    .withColumn("prev_year_revenue", F.lag("monthly_revenue").over(w_same_month))
    .withColumn(
        "year_over_year_growth_pct",
        F.when(
            F.col("prev_year_revenue").isNull() | (F.col("prev_year_revenue") == 0), F.lit(None)
        ).otherwise(
            (F.col("monthly_revenue") - F.col("prev_year_revenue")) / F.col("prev_year_revenue") * 100
        )
    )
    .drop("year_month_key").orderBy("year", "month")
)


w_store = Window.orderBy(F.col("total_revenue").desc())
w_loc = Window.partitionBy("store_city", "store_country")
w_country_store = Window.partitionBy("store_country")

marts_store = (
    base.groupBy("store_id", "store_name", "store_city", "store_country")
    .agg(
        F.sum("sale_total_price").alias("total_revenue"), F.count("sale_id").alias("order_count"),
    )
    .withColumn("avg_check", F.col("total_revenue") / F.col("order_count"))
    .withColumn("location_total_revenue", F.sum("total_revenue").over(w_loc))
    .withColumn("location_total_orders", F.sum("order_count").over(w_loc))
    .withColumn("country_total_revenue", F.sum("total_revenue").over(w_country_store))
    .withColumn("country_total_orders", F.sum("order_count").over(w_country_store))
    .withColumn("is_top5_revenue", (F.row_number().over(w_store) <= 5).cast("int"))
)


w_sup = Window.orderBy(F.col("total_revenue").desc())
w_sup_country = Window.partitionBy("supplier_country")

marts_supplier = (
    base.groupBy("supplier_id", "supplier_name", "supplier_country")
    .agg(
        F.sum("sale_total_price").alias("total_revenue"), F.avg("product_price").alias("avg_product_price"),
        F.sum("sale_quantity").alias("total_qty_sold"), F.count("sale_id").alias("order_count"),
    )
    .withColumn("country_total_revenue", F.sum("total_revenue").over(w_sup_country))
    .withColumn("country_total_orders", F.sum("order_count").over(w_sup_country))
    .withColumn("country_supplier_count", F.count("supplier_id").over(w_sup_country))
    .withColumn("is_top5_revenue", (F.row_number().over(w_sup) <= 5).cast("int"))
)


prod_quality = (
    base.groupBy("product_id", "product_name", "product_category", "product_brand", "product_rating")
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.sum("product_reviews").alias("total_reviews"),
        F.sum("sale_quantity").alias("total_qty_sold"),
    )
)

corr_rating_revenue = prod_quality.stat.corr("product_rating", "total_revenue")
corr_rating_qty = prod_quality.stat.corr("product_rating", "total_qty_sold")

w_global = Window.partitionBy()

marts_quality = (
    prod_quality
    .withColumn("max_rating", F.max("product_rating").over(w_global))
    .withColumn("min_rating", F.min("product_rating").over(w_global))
    .withColumn("max_reviews", F.max("total_reviews").over(w_global))
    .withColumn("is_highest_rating", (F.col("product_rating") == F.col("max_rating")).cast("int"))
    .withColumn("is_lowest_rating", (F.col("product_rating") == F.col("min_rating")).cast("int"))
    .withColumn("is_most_reviews", (F.col("total_reviews") == F.col("max_reviews")).cast("int"))
    .withColumn("rating_revenue_correlation", F.lit(float(round(corr_rating_revenue, 4))).cast("double"))
    .withColumn("rating_qty_correlation", F.lit(float(round(corr_rating_qty, 4))).cast("double"))
    .drop("max_rating", "min_rating", "max_reviews")
    .select(
        "product_id", "product_name", "product_category", "product_brand", "product_rating",
        "total_reviews", "total_revenue", "total_qty_sold", "is_highest_rating",
        "is_lowest_rating", "is_most_reviews", "rating_revenue_correlation", "rating_qty_correlation",
    )
)


def run_clickhouse_query(query: str) -> None:
    credentials = f"{CH_PROPS['user']}:{CH_PROPS['password']}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    req = urllib.request.Request(
        CH_BASE_URL + "/", data=query.encode("utf-8"),
        headers={
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "text/plain; charset=utf-8",
        }, method="POST",
    )
    urllib.request.urlopen(req, timeout=30)

queries = [
    f"CREATE DATABASE IF NOT EXISTS {CH_DB}",

    f"DROP TABLE IF EXISTS {CH_DB}.marts_product",
    f"""CREATE TABLE {CH_DB}.marts_product (
        product_id Int64, product_name String, product_category String, product_brand String,
        total_revenue Float64, total_qty_sold Int64, avg_rating Float64, total_reviews Int64,
        category_total_revenue Float64, category_total_qty_sold Int64, is_top10_sold UInt8
    ) ENGINE = MergeTree() ORDER BY product_id""",

    f"DROP TABLE IF EXISTS {CH_DB}.marts_customer",
    f"""CREATE TABLE {CH_DB}.marts_customer (
        customer_id Int64, customer_first_name String, customer_last_name String,
        customer_country String, total_spend Float64, order_count Int64, avg_check Float64,
        country_total_spend Float64, country_customer_count Int64, is_top10_spend UInt8
    ) ENGINE = MergeTree() ORDER BY customer_id""",

    f"DROP TABLE IF EXISTS {CH_DB}.marts_time",
    f"""CREATE TABLE {CH_DB}.marts_time (
        year Int32, month Int32, month_name String, monthly_revenue Float64,
        yearly_revenue Float64, total_qty Int64, order_count Int64, avg_order_size Float64,
        prev_month_revenue Nullable(Float64), month_over_month_growth_pct Nullable(Float64),
        prev_year_revenue Nullable(Float64), year_over_year_growth_pct Nullable(Float64)
    ) ENGINE = MergeTree() ORDER BY (year, month)""",

    f"DROP TABLE IF EXISTS {CH_DB}.marts_store",
    f"""CREATE TABLE {CH_DB}.marts_store (
        store_id Int64, store_name String, store_city String, store_country String,
        total_revenue Float64, order_count Int64, avg_check Float64, location_total_revenue Float64,
        location_total_orders Int64, country_total_revenue Float64, country_total_orders Int64, is_top5_revenue UInt8
    ) ENGINE = MergeTree() ORDER BY store_id""",

    f"DROP TABLE IF EXISTS {CH_DB}.marts_supplier",
    f"""CREATE TABLE {CH_DB}.marts_supplier (
        supplier_id Int64, supplier_name String, supplier_country String, total_revenue Float64,
        avg_product_price Float64, total_qty_sold Int64, order_count Int64, country_total_revenue Float64,
        country_total_orders Int64, country_supplier_count Int64, is_top5_revenue UInt8
    ) ENGINE = MergeTree() ORDER BY supplier_id""",

    f"DROP TABLE IF EXISTS {CH_DB}.marts_quality",
    f"""CREATE TABLE {CH_DB}.marts_quality (
        product_id Int64, product_name String, product_category String, product_brand String,
        product_rating Float64, total_reviews Int64, total_revenue Float64, total_qty_sold Int64,
        is_highest_rating UInt8, is_lowest_rating UInt8, is_most_reviews UInt8,
        rating_revenue_correlation Nullable(Float64), rating_qty_correlation Nullable(Float64)
    ) ENGINE = MergeTree() ORDER BY product_id""",
]

print("Creating ClickHouse tables...")
for i, q in enumerate(queries, 1):
    run_clickhouse_query(q)
    if q.startswith("CREATE TABLE"):
        print(f"{i}/{len(queries)}: table created")


def write(df, table: str) -> None:
    df.write.mode("append").jdbc(url=CH_URL, table=f"{CH_DB}.{table}", properties=CH_PROPS,)

write(marts_product, "marts_product")
write(marts_customer, "marts_customer")
write(marts_time, "marts_time")
write(marts_store, "marts_store")
write(marts_supplier, "marts_supplier")
write(marts_quality, "marts_quality")

print("OK: all marts written to ClickHouse")

spark.stop()
