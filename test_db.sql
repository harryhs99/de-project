DROP DATABASE IF EXISTS test_tote_db;
CREATE DATABASE test_tote_db;
CREATE USER user123 WITH ENCRYPTED PASSWORD 'password123';
GRANT ALL PRIVILEGES ON test_tote_db TO user123;
\c test_tote_db
CREATE TABLE dim_counterparty (
    counterparty_id INT PRIMARY KEY,
  counterparty_legal_name VARCHAR(240),
  counterparty_legal_address_line_1 VARCHAR(240),
  counterparty_legal_address_line_2 VARCHAR(240),
  counterparty_legal_district VARCHAR(240),
  counterparty_legal_city VARCHAR(240),
  counterparty_legal_postal_code VARCHAR(240),
  counterparty_legal_country VARCHAR(240),
  counterparty_legal_phone_number VARCHAR(240)
);
GRANT ALL PRIVILEGES ON dim_counterparty TO user123;
CREATE TABLE  dim_currency (
  currency_id INT PRIMARY KEY,
  currency_code VARCHAR,
  currency_name VARCHAR
);
GRANT ALL PRIVILEGES ON dim_currency TO user123;
CREATE TABLE dim_date (
  date_id DATE PRIMARY KEY,
  year INT,
  month INT,
  day INT, 
  day_of_week INT,
  day_name VARCHAR, 
  month_name VARCHAR,
  quarter INT 
);
GRANT ALL PRIVILEGES ON dim_date TO user123;
CREATE TABLE dim_design (
  design_id INT PRIMARY KEY, 
  design_name VARCHAR,
  file_location VARCHAR,
  file_name VARCHAR
);
GRANT ALL PRIVILEGES ON dim_design TO user123;
CREATE TABLE dim_location (
  location_id INT PRIMARY KEY, 
  address_line_1 VARCHAR,
  address_line_2 VARCHAR,
  district VARCHAR,
  city VARCHAR,
  postal_code VARCHAR,
  country VARCHAR,
  phone VARCHAR
);
GRANT ALL PRIVILEGES ON dim_location TO user123;
CREATE TABLE dim_staff (
  staff_id INT PRIMARY KEY, 
  first_name VARCHAR,
  last_name VARCHAR,
  department_name VARCHAR,
  location VARCHAR,
  email_address VARCHAR
);
GRANT ALL PRIVILEGES ON dim_staff TO user123;