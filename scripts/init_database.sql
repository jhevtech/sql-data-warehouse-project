/*
******************************************************
Create Database and schemas
******************************************************
Script Purpose:
  This script creates a new database named 'DataWarehouse'.
  Sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/

**Using Postgresql admin**

--  Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
