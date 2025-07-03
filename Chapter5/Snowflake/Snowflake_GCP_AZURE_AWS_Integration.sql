CREATE DATABASE IF NOT EXISTS INTEGRATION_TESTING

USE DATABASE INTEGRATION_TESTING


---------- AWS Integracja


CREATE SCHEMA IF NOT EXISTS AWS_SCHEMA;

USE SCHEMA AWS_SCHEMA;


-- Tabela employees
CREATE OR REPLACE TABLE employees (  
    employee_id INT,  
    first_name VARCHAR(50),  
    last_name VARCHAR(50),  
    email VARCHAR(100),  
    department VARCHAR(50),  
    salary DECIMAL(10,2),  
    hire_date DATE  
);

-- Storage integration dla AWS S3  
CREATE OR REPLACE STORAGE INTEGRATION s3_integration  
  TYPE = EXTERNAL_STAGE  
  STORAGE_PROVIDER = 'S3'  
  ENABLED = TRUE  
  STORAGE_AWS_ROLE_ARN = 'AWS_ROLE_ARN'  
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-s3-integration-bucket');


-- Opis integracji
DESC STORAGE INTEGRATION s3_integration;

-- Nowy stage
CREATE OR REPLACE STAGE s3_stage  
  STORAGE_INTEGRATION = s3_integration  
  URL = 's3://snowflake-s3-integration-bucket'  
  FILE_FORMAT = (  
    TYPE = 'CSV'  
    FIELD_DELIMITER = ','  
    SKIP_HEADER = 1  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
    ESCAPE_UNENCLOSED_FIELD = NONE  
  );

-- Pliki w stage
LIST @s3_stage;
  
-- Kopiowanie danych do tabeli
COPY INTO employees  
FROM @s3_stage/employees.csv  
FILE_FORMAT = (  
    TYPE = 'CSV'  
    FIELD_DELIMITER = ','  
    SKIP_HEADER = 1  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
);

-- Tabela po zaladowaniu
SELECT * FROM employees


---------- Azure Integracja

CREATE SCHEMA IF NOT EXISTS AZURE_SCHEMA;

USE SCHEMA AZURE_SCHEMA;



-- Tabela employees
CREATE OR REPLACE TABLE employees (  
    employee_id INT,  
    first_name VARCHAR(50),  
    last_name VARCHAR(50),  
    email VARCHAR(100),  
    department VARCHAR(50),  
    salary DECIMAL(10,2),  
    hire_date DATE  
);



-- Storage integration dla Azure
CREATE OR REPLACE STORAGE INTEGRATION azure_integration  
  TYPE = EXTERNAL_STAGE  
  STORAGE_PROVIDER = 'AZURE'  
  ENABLED = TRUE  
  AZURE_TENANT_ID = 'AZURE_TENANT_ID'  
  STORAGE_ALLOWED_LOCATIONS = ('azure://snowflakeintegrationacc.blob.core.windows.net/snowflake-container');


-- Opis integracji
DESC STORAGE INTEGRATION azure_integration;



-- Nowy stage
CREATE OR REPLACE STAGE azure_stage  
  STORAGE_INTEGRATION = azure_integration  
  URL = 'azure://snowflakeintegrationacc.blob.core.windows.net/snowflake-container'  
  FILE_FORMAT = (  
    TYPE = 'CSV'  
    FIELD_DELIMITER = ','  
    SKIP_HEADER = 1  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
    ESCAPE_UNENCLOSED_FIELD = NONE  
  );

-- Pliki w stage
LIST @azure_stage;
  
-- Kopiowanie danych do tabeli
COPY INTO employees  
FROM @azure_stage/employees.csv  
FILE_FORMAT = (  
    TYPE = 'CSV'  
    FIELD_DELIMITER = ','  
    SKIP_HEADER = 1  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
);

-- Tabela po zaladowaniu
SELECT * FROM employees




---------- GCP integracja


CREATE SCHEMA IF NOT EXISTS GCP_SCHEMA;

USE SCHEMA GCP_SCHEMA;


-- Tabela employees
CREATE OR REPLACE TABLE employees (  
    employee_id INT,  
    first_name VARCHAR(50),  
    last_name VARCHAR(50),  
    email VARCHAR(100),  
    department VARCHAR(50),  
    salary DECIMAL(10,2),  
    hire_date DATE  
);



-- Storage integration dla Azure
CREATE OR REPLACE STORAGE INTEGRATION gcp_integration  
  TYPE = EXTERNAL_STAGE  
  STORAGE_PROVIDER = 'GCS'  
  ENABLED = TRUE  
  STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-bucket-test');


-- Opis integracji
DESC STORAGE INTEGRATION gcp_integration;


-- Nowy stage
CREATE OR REPLACE STAGE gcp_stage  
  STORAGE_INTEGRATION = gcp_integration  
  URL = 'gcs://snowflake-bucket-test'  
  FILE_FORMAT = (  
    TYPE = 'CSV'  
    FIELD_DELIMITER = ','  
    SKIP_HEADER = 1  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
    ESCAPE_UNENCLOSED_FIELD = NONE  
  );

-- Pliki w stage
LIST @gcp_stage;
  
-- Kopiowanie danych do tabeli
COPY INTO employees  
FROM @gcp_stage/employees.csv  
FILE_FORMAT = (  
    TYPE = 'CSV'  
    FIELD_DELIMITER = ','  
    SKIP_HEADER = 1  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
);

-- Tabela po zaladowaniu
SELECT * FROM employees










