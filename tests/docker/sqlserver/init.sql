CREATE DATABASE testdb;
GO

USE testdb;
GO

CREATE TABLE test_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    value INT DEFAULT 0,
    created_at DATETIME2 DEFAULT GETDATE()
);
GO

INSERT INTO test_table (name, value) VALUES ('test1', 100), ('test2', 200);
GO
