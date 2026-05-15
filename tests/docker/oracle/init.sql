CREATE TABLE test_table (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    value NUMBER DEFAULT 0,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

INSERT INTO test_table (name, value) VALUES ('test1', 100);
INSERT INTO test_table (name, value) VALUES ('test2', 200);
COMMIT;
