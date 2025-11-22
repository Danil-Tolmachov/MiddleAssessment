IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'etl_test')
BEGIN

CREATE DATABASE etl_test;

END;

USE etl_test;