--------- Tworzenie bazy danych i nowych schematów
CREATE DATABASE snowflake_sales_dwh;  

USE DATABASE snowflake_sales_dwh;

CREATE SCHEMA raw_data;  
CREATE SCHEMA staging;  
CREATE SCHEMA marts;


---------  Nowa integracja Amazon S3
USE SCHEMA raw_data;


CREATE OR REPLACE STORAGE INTEGRATION s3_integration  
  TYPE = EXTERNAL_STAGE  
  STORAGE_PROVIDER = 'S3'  
  ENABLED = TRUE  
  STORAGE_AWS_ROLE_ARN = 'AWS_ROLE_ARN'  
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-dwh-project');

DESC STORAGE INTEGRATION s3_integration;

---------  Tworzenie nowych stage

CREATE OR REPLACE FILE FORMAT parquet_format  
  TYPE = 'PARQUET';

-- Sales stage
CREATE OR REPLACE STAGE sales_stage  
  STORAGE_INTEGRATION = s3_integration  
  URL = 's3://snowflake-dwh-project/raw_data/sales/'  
  FILE_FORMAT = parquet_format;

-- Customer stage 
CREATE OR REPLACE STAGE customers_stage  
  STORAGE_INTEGRATION = s3_integration  
  URL = 's3://snowflake-dwh-project/raw_data/customers/'  
  FILE_FORMAT = parquet_format;

-- Products stage
CREATE OR REPLACE STAGE products_stage  
  STORAGE_INTEGRATION = s3_integration  
  URL = 's3://snowflake-dwh-project/raw_data/products/'  
  FILE_FORMAT = parquet_format;

-- Orders stage
CREATE OR REPLACE STAGE orders_stage  
  STORAGE_INTEGRATION = s3_integration  
  URL = 's3://snowflake-dwh-project/raw_data/orders/'  
  FILE_FORMAT = parquet_format;


-- Suppliers stage
CREATE OR REPLACE STAGE suppliers_stage  
  STORAGE_INTEGRATION = s3_integration  
  URL = 's3://snowflake-dwh-project/raw_data/suppliers/'  
  FILE_FORMAT = parquet_format; 

/*
-- Sprawdzenie istniej¹cych plików
LIST @sales_stage;
LIST @customers_stage;
LIST @products_stage;
LIST @orders_stage;
LIST @suppliers_stage;
*/


--------- Tworzenie nowych tabel RAW

-- Tabela RAW danych sprzeda¿y  
CREATE OR REPLACE TABLE raw_sales (
    sale_id STRING,
    record_data VARIANT,
    file_name STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    meta_file_last_modified TIMESTAMP_NTZ
);


-- Tabela RAW dla klientów  
CREATE OR REPLACE TABLE raw_customers (
    customer_id STRING,
    record_data VARIANT,
    file_name STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    meta_file_last_modified TIMESTAMP_NTZ
);
  
-- Tabela RAW dla produktów 
CREATE OR REPLACE TABLE raw_products (
    product_id STRING,
    record_data VARIANT,
    file_name STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    meta_file_last_modified TIMESTAMP_NTZ
);

-- Tabela RAW dla zamówieñ 
CREATE OR REPLACE TABLE raw_orders (
    order_id STRING,
    record_data VARIANT,
    file_name STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    meta_file_last_modified TIMESTAMP_NTZ
);

  
-- Tabela RAW dla dostawców  
CREATE OR REPLACE TABLE raw_suppliers (
    supplier_id STRING,
    record_data VARIANT,
    file_name STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    meta_file_last_modified TIMESTAMP_NTZ
);

--------- Tworzenie nowych snowpipe

-- Snowpipe dla danych sprzeda¿y  
CREATE OR REPLACE PIPE sales_pipe  
  AUTO_INGEST = TRUE  
  AS  
  COPY INTO raw_data.raw_sales  
  FROM (  
    SELECT  
      $1:sale_id::STRING,  
      $1, 
      METADATA$FILENAME,  
      CURRENT_TIMESTAMP(),  
      METADATA$FILE_LAST_MODIFIED  
    FROM @sales_stage (FILE_FORMAT => parquet_format)  
  );
  
-- Snowpipe dla klientów  
CREATE OR REPLACE PIPE customers_pipe  
  AUTO_INGEST = TRUE  
  AS  
  COPY INTO raw_data.raw_customers  
  FROM (  
    SELECT  
      $1:customer_id::STRING,  
      $1,
      METADATA$FILENAME,  
      CURRENT_TIMESTAMP(),  
      METADATA$FILE_LAST_MODIFIED  
    FROM @customers_stage (FILE_FORMAT => parquet_format)  
  );
  
-- Snowpipe dla produktów  
CREATE OR REPLACE PIPE products_pipe  
  AUTO_INGEST = TRUE  
  AS  
  COPY INTO raw_data.raw_products  
  FROM (  
    SELECT  
      $1:product_id::STRING,  
      $1,  
      METADATA$FILENAME,  
      CURRENT_TIMESTAMP(),  
      METADATA$FILE_LAST_MODIFIED  
    FROM @products_stage (FILE_FORMAT => parquet_format)  
  ); 
  
-- Snowpipe dla zamówieñ  
CREATE OR REPLACE PIPE orders_pipe  
  AUTO_INGEST = TRUE  
  AS  
  COPY INTO raw_data.raw_orders  
  FROM (  
    SELECT  
      $1:order_id::STRING,  
      $1,  
      METADATA$FILENAME,  
      CURRENT_TIMESTAMP(),  
      METADATA$FILE_LAST_MODIFIED  
    FROM @orders_stage (FILE_FORMAT => parquet_format)  
  );

  
-- Snowpipe dla dostawców  
CREATE OR REPLACE PIPE suppliers_pipe  
  AUTO_INGEST = TRUE  
  AS  
  COPY INTO raw_data.raw_suppliers  
  FROM (  
    SELECT  
      $1:supplier_id::STRING,  
      $1, 
      METADATA$FILENAME,  
      CURRENT_TIMESTAMP(),  
      METADATA$FILE_LAST_MODIFIED  
    FROM @suppliers_stage (FILE_FORMAT => parquet_format)  
  );

--------- Manualne odœwie¿enie pipe'ów

ALTER PIPE sales_pipe REFRESH;
ALTER PIPE customers_pipe  REFRESH;  
ALTER PIPE products_pipe  REFRESH;  
ALTER PIPE orders_pipe  REFRESH;  
ALTER PIPE suppliers_pipe REFRESH;


--------- Warstwa stagingowa - utworzenie nowych widoków i transformacja danych
USE SCHEMA staging;



CREATE OR REPLACE VIEW staging.stg_sales_v AS  
SELECT  
    sale_id,  
    record_data:customer_id::STRING          AS customer_id,  
    record_data:product_id::STRING           AS product_id,  
    record_data:sale_date::DATE              AS sale_date,  
    record_data:quantity::INTEGER            AS quantity,  
    record_data:unit_price::DECIMAL(10,2)    AS unit_price,  
    record_data:total_amount::DECIMAL(12,2)  AS total_amount,  
    record_data:sales_rep::STRING            AS sales_rep,  
    record_data:region::STRING               AS region,  
    file_name,  
    load_timestamp,  
    meta_file_last_modified  
FROM raw_data.raw_sales;  
  
CREATE OR REPLACE VIEW staging.stg_customers_v AS  
SELECT  
    customer_id,  
    record_data:customer_name::STRING        AS customer_name,  
    record_data:email::STRING                AS email,  
    record_data:phone::STRING                AS phone,  
    record_data:address::STRING              AS address,  
    record_data:city::STRING                 AS city,  
    record_data:country::STRING              AS country,  
    record_data:registration_date::DATE      AS registration_date,  
    file_name,  
    load_timestamp,  
    meta_file_last_modified  
FROM raw_data.raw_customers;  
  
CREATE OR REPLACE VIEW staging.stg_products_v AS  
SELECT  
    product_id,  
    record_data:product_name::STRING         AS product_name,  
    record_data:category::STRING             AS category,  
    record_data:subcategory::STRING          AS subcategory,  
    record_data:brand::STRING                AS brand,  
    record_data:cost_price::DECIMAL(10,2)    AS cost_price,  
    record_data:list_price::DECIMAL(10,2)    AS list_price,  
    record_data:supplier_id::STRING          AS supplier_id,  
    file_name,  
    load_timestamp,  
    meta_file_last_modified  
FROM raw_data.raw_products;
  
CREATE OR REPLACE VIEW staging.stg_orders_v AS  
SELECT  
    order_id,  
    record_data:customer_id::STRING          AS customer_id,  
    record_data:order_date::DATE             AS order_date,  
    record_data:order_status::STRING         AS order_status,  
    record_data:total_amount::DECIMAL(12,2)  AS total_amount,  
    record_data:shipping_address::STRING     AS shipping_address,  
    file_name,  
    load_timestamp,  
    meta_file_last_modified  
FROM raw_data.raw_orders;  
  
CREATE OR REPLACE VIEW staging.stg_suppliers_v AS  
SELECT  
    supplier_id,  
    record_data:supplier_name::STRING        AS supplier_name,  
    record_data:contact_name::STRING         AS contact_name,  
    record_data:contact_email::STRING        AS contact_email,  
    record_data:phone::STRING                AS phone,  
    record_data:address::STRING              AS address,  
    record_data:city::STRING                 AS city,  
    record_data:country::STRING              AS country,  
    file_name,  
    load_timestamp,  
    meta_file_last_modified  
FROM raw_data.raw_suppliers;

-- Sprawdzenie poprawnoœci parsowania danych
SELECT TOP 100* FROM staging.stg_sales_v;
SELECT TOP 100* FROM staging.stg_customers_v;
SELECT TOP 100* FROM staging.stg_products_v;
SELECT TOP 100* FROM staging.stg_orders_v ;
SELECT TOP 100* FROM staging.stg_suppliers_v;

--------- Warstwa marts - tworzenie nowych tabeli dynamicznych i dodatkowej tabeli z datami
USE SCHEMA MARTS;


CREATE OR REPLACE DYNAMIC TABLE marts.dim_order  
    TARGET_LAG = '5 minutes'  
    WAREHOUSE  = 'COMPUTE_WH'
    REFRESH_MODE='Incremental' 
AS  
SELECT  
    order_id,  
    customer_id,  
    order_date,  
    order_status,  
    total_amount,  
    shipping_address,  
    file_name,  
    load_timestamp,  
    meta_file_last_modified,  
FROM staging.stg_orders_v  
QUALIFY ROW_NUMBER() OVER (  
          PARTITION BY order_id  
          ORDER BY meta_file_last_modified DESC  
       ) = 1;

CREATE OR REPLACE DYNAMIC TABLE marts.dim_supplier  
    TARGET_LAG = '5 minutes'  
    WAREHOUSE  = 'COMPUTE_WH'
    REFRESH_MODE='Incremental' 
AS  
SELECT  
    supplier_id,  
    supplier_name,  
    contact_name,  
    contact_email,  
    phone,  
    address,  
    city,  
    country,  
    file_name,  
    load_timestamp,  
    meta_file_last_modified,  
FROM staging.stg_suppliers_v
QUALIFY ROW_NUMBER() OVER (  
          PARTITION BY supplier_id  
          ORDER BY meta_file_last_modified DESC  
       ) = 1;

CREATE OR REPLACE DYNAMIC TABLE marts.dim_customer  
    TARGET_LAG = '5 minutes'  
    WAREHOUSE  = 'COMPUTE_WH'
    REFRESH_MODE='Incremental'
AS  
SELECT  
    cust.customer_id,  
    customer_name,  
    email,  
    city,  
    country,  
    registration_date,
    cust.file_name,  
    cust.load_timestamp,  
    cust.meta_file_last_modified,
    MAX(ord.order_date) last_customer_order_date,
    
FROM staging.stg_customers_v cust
LEFT JOIN marts.dim_order ord
ON cust.customer_id=ord.customer_id
GROUP BY ALL
QUALIFY ROW_NUMBER() OVER (  
          PARTITION BY cust.customer_id  
          ORDER BY cust.meta_file_last_modified DESC  
       ) = 1;


CREATE OR REPLACE DYNAMIC TABLE marts.dim_product  
    TARGET_LAG = '5 minutes'  
    WAREHOUSE  = 'COMPUTE_WH'
    REFRESH_MODE='Incremental'
AS  
SELECT  
    p.product_id,  
    product_name,  
    category,  
    subcategory,  
    brand,  
    cost_price,  
    list_price,  
    p.supplier_id,  
    s.supplier_name,
    p.file_name,  
    p.load_timestamp,  
    p.meta_file_last_modified,
FROM staging.stg_products_v p
LEFT JOIN marts.dim_supplier  s
ON p.supplier_id=s.supplier_id
QUALIFY ROW_NUMBER() OVER (  
          PARTITION BY p.product_id  
          ORDER BY p.meta_file_last_modified DESC  
       ) = 1;



-- Tabela z datami
CREATE OR REPLACE  TABLE marts.dim_date  
AS
WITH date_spine AS (  
  SELECT   
    DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '2020-01-01'::DATE) AS date_value  
  FROM TABLE(GENERATOR(ROWCOUNT => 3653))  
)  
SELECT  
    TO_NUMBER(TO_CHAR(date_value, 'YYYYMMDD')) AS date_key,  
    date_value,  
    YEAR(date_value) AS year,  
    QUARTER(date_value) AS quarter,  
    MONTH(date_value) AS month,  
    DAY(date_value) AS day,  
    DAYOFWEEK(date_value) AS day_of_week,  
    WEEKOFYEAR(date_value) AS week_of_year,  
    CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,  
    TO_CHAR(date_value, 'MMMM') AS month_name,  
    TO_CHAR(date_value, 'DY') AS day_name,  
    CASE   
        WHEN MONTH(date_value) IN (12, 1, 2) THEN 'Winter'  
        WHEN MONTH(date_value) IN (3, 4, 5) THEN 'Spring'  
        WHEN MONTH(date_value) IN (6, 7, 8) THEN 'Summer'  
        ELSE 'Autumn'  
    END AS season,
    CURRENT_TIMESTAMP() AS processed_at
FROM date_spine  
WHERE date_value <= '2030-12-31'; 


--------- Finalna tabela faktów 
CREATE OR REPLACE DYNAMIC TABLE marts.fact_sales  
    TARGET_LAG = '5 minutes'  
    WAREHOUSE  = 'COMPUTE_WH'
    REFRESH_MODE='Incremental' 
AS  
SELECT  
    s.sale_id,  
    dc.customer_id AS customer_key,  
    dp.product_id AS product_key,  
    dd.date_key,  
    s.quantity,  
    s.unit_price,  
    s.total_amount,  
    s.sales_rep,  
    s.region,
    s.sale_date,
    s.file_name,
    dc.last_customer_order_date,
    dp.supplier_name,
    s.load_timestamp,  
    s.meta_file_last_modified,
FROM (  
    SELECT *  
    FROM staging.stg_sales_v  
    QUALIFY ROW_NUMBER() OVER (  
              PARTITION BY sale_id  
              ORDER BY meta_file_last_modified DESC  
           ) = 1  
) s  
LEFT JOIN marts.dim_customer dc ON s.customer_id = dc.customer_id  
LEFT JOIN marts.dim_product dp ON s.product_id = dp.product_id  
LEFT JOIN marts.dim_date dd ON s.sale_date = dd.date_value


-- Tabele mart
SELECT TOP 100* FROM marts.fact_sales;
SELECT TOP 100* FROM marts.dim_customer;
SELECT TOP 100* FROM marts.dim_date;
SELECT TOP 100* FROM marts.dim_supplier;
SELECT TOP 100* FROM marts.dim_order;
SELECT TOP 100* FROM marts.dim_product;


