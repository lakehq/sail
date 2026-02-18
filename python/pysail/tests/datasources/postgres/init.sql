-- Initialize test database with sample data

-- Create a simple users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data (including NULL values and edge cases)
INSERT INTO users (name, email, age, is_active) VALUES
    ('Alice Johnson', 'alice@example.com', 28, TRUE),
    ('Bob Smith', 'bob@example.com', 35, TRUE),
    ('Charlie Brown', 'charlie@example.com', 42, FALSE),
    ('Diana Prince', 'diana@example.com', 31, TRUE),
    ('Eve Davis', 'eve@example.com', 27, TRUE),
    ('Frank Miller', 'frank@example.com', 45, FALSE),
    ('Grace Lee', 'grace@example.com', 29, TRUE),
    ('Henry Wilson', 'henry@example.com', 38, TRUE),
    ('Ivy Chen', 'ivy@example.com', 33, TRUE),
    ('Jack Ryan', 'jack@example.com', 41, FALSE),
    ('NULL Age User', 'null@example.com', NULL, TRUE),
    ('张伟', 'zhang@example.com', 25, TRUE),  -- Unicode: Chinese
    ('José García', 'jose@example.com', 30, TRUE),  -- Unicode: Spanish
    ('محمد علي', 'mohamed@example.com', 35, TRUE),  -- Unicode: Arabic
    ('😀 Emoji User', 'emoji@example.com', 22, TRUE);  -- Unicode: Emoji

-- Create a products table for more complex testing
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DOUBLE PRECISION,
    stock_quantity INTEGER,
    last_updated DATE
);

-- Insert product data
INSERT INTO products (product_name, category, price, stock_quantity, last_updated) VALUES
    ('Laptop Pro 15', 'Electronics', 1299.99, 50, '2024-01-15'),
    ('Wireless Mouse', 'Electronics', 29.99, 200, '2024-01-20'),
    ('Office Chair', 'Furniture', 249.99, 30, '2024-02-01'),
    ('Desk Lamp', 'Furniture', 45.50, 100, '2024-02-05'),
    ('Coffee Maker', 'Appliances', 89.99, 75, '2024-01-25'),
    ('Notebook Set', 'Stationery', 12.99, 500, '2024-02-10'),
    ('USB-C Cable', 'Electronics', 15.99, 300, '2024-02-12'),
    ('Standing Desk', 'Furniture', 399.99, 20, '2024-01-18'),
    ('Mechanical Keyboard', 'Electronics', 149.99, 80, '2024-02-08'),
    ('Water Bottle', 'Accessories', 19.99, 250, '2024-02-14');

-- Create indexes for better performance
CREATE INDEX idx_users_age ON users(age);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);

-- Create a table for testing various PostgreSQL data types
CREATE TABLE data_types_test (
    id SERIAL PRIMARY KEY,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_real REAL,
    col_double DOUBLE PRECISION,
    col_numeric NUMERIC(10, 2),
    col_text TEXT,
    col_varchar VARCHAR(50),
    col_boolean BOOLEAN,
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_timestamptz TIMESTAMP WITH TIME ZONE,
    col_json JSON,
    col_jsonb JSONB,
    col_uuid UUID,
    col_bytea BYTEA
);

-- Insert test data with various types
INSERT INTO data_types_test VALUES (
    1,
    100::SMALLINT,
    10000::INTEGER,
    1000000000::BIGINT,
    3.14::REAL,
    2.718281828::DOUBLE PRECISION,
    12345.67::NUMERIC,
    'Sample text',
    'Sample varchar',
    TRUE,
    '2024-01-15'::DATE,
    '2024-01-15 10:30:00'::TIMESTAMP,
    '2024-01-15 10:30:00-05:00'::TIMESTAMPTZ,
    '{"key": "value"}'::JSON,
    '{"key": "value", "num": 123}'::JSONB,
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID,
    '\xDEADBEEF'::BYTEA
);

-- Insert row with NULLs
INSERT INTO data_types_test (id) VALUES (2);

-- Create an empty table for testing
CREATE TABLE empty_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

-- Create a large table for performance testing
CREATE TABLE large_table (
    id SERIAL PRIMARY KEY,
    value INTEGER,
    text_data VARCHAR(100)
);

-- Insert 10,000 rows
INSERT INTO large_table (value, text_data)
SELECT
    i,
    'Row ' || i::TEXT
FROM generate_series(1, 10000) AS i;

-- Create table for JOIN testing
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER,
    order_date DATE
);

-- Insert order data
INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES
    (1, 1, 2, '2024-01-10'),
    (1, 3, 1, '2024-01-15'),
    (2, 2, 3, '2024-01-12'),
    (3, 1, 1, '2024-01-14'),
    (3, 4, 2, '2024-01-16'),
    (4, 5, 1, '2024-01-11'),
    (5, 3, 4, '2024-01-13'),
    (5, 6, 2, '2024-01-17');

-- Create table with special characters for SQL injection testing
CREATE TABLE special_chars (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    description TEXT
);

INSERT INTO special_chars (name, description) VALUES
    ('Normal Name', 'Normal description'),
    ('O''Reilly', 'Name with apostrophe'),
    ('Quote"Test', 'Name with double quote'),
    ('Line
Break', 'Text with newline'),
    ('Tab	Test', 'Text with tab'),
    ('Backslash\Test', 'Text with backslash');

-- Create a non-public schema for schema isolation testing
CREATE SCHEMA test_schema;
GRANT USAGE ON SCHEMA test_schema TO testuser;

CREATE TABLE test_schema.schema_table (
    id SERIAL PRIMARY KEY,
    value TEXT
);
GRANT SELECT ON test_schema.schema_table TO testuser;

INSERT INTO test_schema.schema_table (value) VALUES
    ('row_from_test_schema_1'),
    ('row_from_test_schema_2'),
    ('row_from_test_schema_3');
