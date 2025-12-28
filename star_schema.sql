CREATE SCHEMA WalmartDW;
SET search_path to WalmartDW;

CREATE TYPE gender_enum AS ENUM ('M', 'F');
CREATE TYPE marital_enum AS ENUM ('0', '1');
CREATE TYPE age_group_enum AS ENUM (
    '0-17',
    '18-25',
    '26-35',
    '36-45',
    '46-50',
    '51-55',
    '55+'
);

CREATE TABLE Customer(
customer_id INT PRIMARY KEY,
gender gender_enum NOT NULL,
age_group age_group_enum NOT NULL,
occupation INT NOT NULL,
city_category varchar(1) NOT NULL,
marital_status marital_enum NOT NULL,
stay_in_current_city_years INT NOT NULL,
check(stay_in_current_city_years >= 0)
);

CREATE TABLE Product (
product_id TEXT NOT NULL PRIMARY KEY,
product_category TEXT NOT NULL,
price NUMERIC(12,2) NOT NULL,
check(price > 0)
);

CREATE TABLE Store (
store_id integer NOT NULL PRIMARY KEY,
storeName varchar NOT NULL
);

CREATE TABLE Supplier(
supplier_id integer NOT NULL PRIMARY KEY,
supplierName varchar NOT NULL
);

CREATE TABLE Date(
date_id integer NOT NULL PRIMARY KEY,
transaction_date date NOT NULL,
dayNum INT NOT NULL,
monthNum INT NOT NULL,
year INT NOT NULL,
dayofweek varchar NOT NULL,
quarter_num INTEGER NOT NULL,
is_weekend BOOLEAN NOT NULL
);

CREATE TABLE Sales(
sales_id integer NOT NULL PRIMARY KEY,
order_id integer NOT NULL,
customer_id integer NOT NULL REFERENCES Customer(customer_id),
product_id TEXT NOT NULL REFERENCES Product(product_id),
date_id integer NOT NULL REFERENCES Date(date_id),
store_id integer NOT NULL REFERENCES Store(store_id),
supplier_id integer NOT NULL REFERENCES Supplier(supplier_id),
sales_amount numeric(12,2) NOT NULL,
quantity integer NOT NULL,
CHECK(quantity >= 0),
CHECK(sales_amount >= 0)
);


