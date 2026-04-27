
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_first_name VARCHAR(50),
    customer_last_name VARCHAR(50),
    customer_age INTEGER,
    customer_email VARCHAR(50)  NOT NULL UNIQUE,
    customer_country VARCHAR(50),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(50),
    customer_pet_breed VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_first_name VARCHAR(50),
    seller_last_name VARCHAR(50),
    seller_email VARCHAR(50) NOT NULL UNIQUE,
    seller_country VARCHAR(50),
    seller_postal_code VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name VARCHAR(50) NOT NULL,
    store_location VARCHAR(50),
    store_city VARCHAR(50),
    store_state VARCHAR(50),
    store_country VARCHAR(50),
    store_phone VARCHAR(50),
    store_email VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_name VARCHAR(50) NOT NULL,
    supplier_contact VARCHAR(50),
    supplier_email VARCHAR(50) NOT NULL UNIQUE,
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(50),
    supplier_city VARCHAR(50),
    supplier_country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name VARCHAR(60) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    pet_category VARCHAR(50),
    product_price NUMERIC(10, 2),
    product_quantity INTEGER,
    product_weight NUMERIC(10, 2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(50) NOT NULL,
    product_material VARCHAR(50),
    product_description TEXT,
    product_rating NUMERIC(3, 2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);


CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mock_data_id BIGINT NOT NULL,
    sale_date DATE,
    customer_id BIGINT REFERENCES dim_customer(customer_id),
    seller_id BIGINT REFERENCES dim_seller(seller_id),
    store_id BIGINT REFERENCES dim_store(store_id),
    supplier_id BIGINT REFERENCES dim_supplier(supplier_id),
    product_id BIGINT REFERENCES dim_product(product_id),
    sale_quantity INTEGER,
    sale_total_price NUMERIC(10, 2)
);


CREATE OR REPLACE VIEW v_country AS
SELECT DISTINCT customer_country AS country FROM dim_customer
UNION SELECT DISTINCT seller_country FROM dim_seller
UNION SELECT DISTINCT store_country FROM dim_store
UNION SELECT DISTINCT supplier_country FROM dim_supplier;
