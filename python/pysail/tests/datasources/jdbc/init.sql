-- Initialise test database for JDBC datasource tests.

-- users table (15 rows: 10 base + 1 null-age + 4 unicode)
CREATE TABLE IF NOT EXISTS users (
    id        SERIAL PRIMARY KEY,
    name      VARCHAR(100) NOT NULL,
    email     VARCHAR(200),
    age       INTEGER,
    active    BOOLEAN DEFAULT TRUE,
    score     DOUBLE PRECISION,
    created   TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email, age, active, score) VALUES
    ('Alice',         'alice@example.com',   30,   TRUE,  9.5),
    ('Bob',           'bob@example.com',     25,   TRUE,  7.2),
    ('Charlie',       'charlie@example.com', 35,   FALSE, 8.8),
    ('Diana',         'diana@example.com',   28,   TRUE,  9.1),
    ('Eve',           'eve@example.com',     22,   TRUE,  6.7),
    ('Frank',         'frank@example.com',   40,   FALSE, 5.0),
    ('Grace',         'grace@example.com',   31,   TRUE,  8.3),
    ('Hank',          'hank@example.com',    27,   TRUE,  7.9),
    ('Ivy',           'ivy@example.com',     33,   TRUE,  9.0),
    ('Jack',          'jack@example.com',    29,   FALSE, 4.5),
    ('NULL Age User', 'null@example.com',    NULL, TRUE,  8.0),
    ('张伟',           'zhang@example.com',   25,   TRUE,  9.2),
    ('José García',   'jose@example.com',    30,   TRUE,  7.5),
    ('محمد علي',      'mohamed@example.com', 35,   TRUE,  8.7),
    ('😀 Emoji User', 'emoji@example.com',   22,   TRUE,  9.9);

-- products table for type-variety testing
CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    price       NUMERIC(10, 2),
    quantity    SMALLINT,
    weight_kg   REAL,
    in_stock    BOOLEAN,
    listed_at   DATE
);

INSERT INTO products (name, price, quantity, weight_kg, in_stock, listed_at) VALUES
    ('Widget A',  9.99,   100, 0.5,  TRUE,  '2024-01-15'),
    ('Widget B',  14.99,  50,  1.2,  TRUE,  '2024-02-01'),
    ('Gadget X',  49.99,  25,  0.3,  TRUE,  '2024-03-10'),
    ('Gadget Y',  29.99,  0,   0.8,  FALSE, '2024-03-11'),
    ('Doohickey', 5.49,   200, 0.1,  TRUE,  '2024-04-20');

-- large_table for partition testing (10 000 rows)
CREATE TABLE IF NOT EXISTS large_table AS
SELECT
    generate_series(1, 10000) AS id,
    md5(random()::text)       AS value;

-- orders table for JOIN testing
CREATE TABLE IF NOT EXISTS orders (
    order_id   SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity   INTEGER,
    order_date DATE
);

INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES
    (1, 1, 2, '2024-01-10'),
    (1, 3, 1, '2024-01-15'),
    (2, 2, 3, '2024-01-12'),
    (3, 1, 1, '2024-01-14'),
    (4, 5, 2, '2024-01-16'),
    (5, 3, 1, '2024-01-11'),
    (6, 2, 4, '2024-01-13'),
    (7, 4, 2, '2024-01-17');

-- empty_table for edge-case testing
CREATE TABLE IF NOT EXISTS empty_table (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

-- special_chars for SQL injection testing
CREATE TABLE IF NOT EXISTS special_chars (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200),
    description TEXT
);

INSERT INTO special_chars (name, description) VALUES
    ('Normal Name',   'Normal description'),
    ('O''Reilly',     'Name with apostrophe'),
    ('Quote"Test',    'Name with double quote'),
    ('Tab	Test',      'Text with tab'),
    ('Backslash\Test','Text with backslash');

-- data_types_test for type coverage testing
CREATE TABLE IF NOT EXISTS data_types_test (
    id            SERIAL PRIMARY KEY,
    col_smallint  SMALLINT,
    col_integer   INTEGER,
    col_bigint    BIGINT,
    col_real      REAL,
    col_double    DOUBLE PRECISION,
    col_text      TEXT,
    col_varchar   VARCHAR(50),
    col_boolean   BOOLEAN,
    col_date      DATE,
    col_timestamp TIMESTAMP
);

INSERT INTO data_types_test VALUES (
    1,
    100::SMALLINT,
    10000::INTEGER,
    1000000000::BIGINT,
    3.14::REAL,
    2.718281828::DOUBLE PRECISION,
    'Sample text',
    'Sample varchar',
    TRUE,
    '2024-01-15'::DATE,
    '2024-01-15 10:30:00'::TIMESTAMP
);

-- row 2: all NULLs except id
INSERT INTO data_types_test (id) VALUES (2);

-- schema_test for schema-qualified name tests
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS analytics.events (
    event_id   SERIAL PRIMARY KEY,
    event_name VARCHAR(100),
    user_id    INTEGER,
    ts         TIMESTAMP DEFAULT NOW()
);
INSERT INTO analytics.events (event_name, user_id) VALUES
    ('page_view', 1),
    ('click',     2),
    ('purchase',  1),
    ('logout',    3);

-- test_schema for tableSchema / schema-isolation testing
CREATE SCHEMA IF NOT EXISTS test_schema;
CREATE TABLE IF NOT EXISTS test_schema.schema_table (
    id    SERIAL PRIMARY KEY,
    value TEXT
);
INSERT INTO test_schema.schema_table (value) VALUES
    ('row_from_test_schema_1'),
    ('row_from_test_schema_2'),
    ('row_from_test_schema_3');
