-- ===========================================================================
-- Create Database and Schemas
-- ===========================================================================

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'CustomerChurn')
BEGIN
	ALTER DATABASE CustomerChurn SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE CustomerChurn;
END;

-- Create the 'CustomerChurn' database
CREATE DATABASE CustomerChurn;
GO

USE CustomerChurn;
GO

-- Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

