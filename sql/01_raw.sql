
CREATE TABLE mock_data (
    id BIGINT,
    customer_first_name VARCHAR(50), customer_last_name VARCHAR(50), customer_age INTEGER, customer_email VARCHAR(50), 
    customer_country VARCHAR(50), customer_postal_code VARCHAR(50), customer_pet_type VARCHAR(50), customer_pet_name VARCHAR(50), 
    customer_pet_breed VARCHAR(50),
    
    seller_first_name VARCHAR(50), seller_last_name VARCHAR(50), seller_email VARCHAR(50), seller_country VARCHAR(50), 
    seller_postal_code VARCHAR(50),
    
    product_name VARCHAR(60), product_category VARCHAR(50), product_price NUMERIC(10,2), product_quantity INTEGER,
    
    sale_date DATE,
    sale_customer_id BIGINT, sale_seller_id BIGINT, sale_product_id BIGINT, sale_quantity INTEGER, 
    sale_total_price NUMERIC(10,2),
    
    store_name VARCHAR(50), store_location VARCHAR(50), store_city VARCHAR(50), store_state VARCHAR(50), store_country VARCHAR(50), 
    store_phone VARCHAR(50), store_email VARCHAR(50),
    
    pet_category VARCHAR(50),
    
    product_weight NUMERIC(10,2), product_color VARCHAR(50), product_size VARCHAR(50), product_brand VARCHAR(50), 
    product_material VARCHAR(50), product_description TEXT, product_rating NUMERIC(3,2), product_reviews INTEGER,
    product_release_date DATE, product_expiry_date DATE,
    
    supplier_name VARCHAR(50), supplier_contact VARCHAR(50), supplier_email VARCHAR(50), supplier_phone VARCHAR(50), 
    supplier_address VARCHAR(50), supplier_city VARCHAR(50), supplier_country VARCHAR(50)
);

COPY mock_data FROM '/raw_data/MOCK_DATA (1).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (2).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (3).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (4).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (5).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (6).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (7).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (8).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA (9).csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');
COPY mock_data FROM '/raw_data/MOCK_DATA.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'UTF8');

DO $$ BEGIN RAISE NOTICE 'Staging loaded: % rows', (SELECT count(*) FROM mock_data); END $$;
