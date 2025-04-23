/*
###################################################################################
            Stored Procedure: Load Bronze Layer (Source -> Bronze)
Script Purpose: 
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'COPY' command which is similar to 'BULK INSERT' to load data from csv 
    files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  CALL bronze.load_bronze;
###################################################################################
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$

--getting time duration
DECLARE
	start_time TIMESTAMP; 
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;

BEGIN 
	batch_start_time := clock_timestamp();
	--crm cust info 
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM 'C:/Users/yuji/OneDrive/Data warehouse project/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;
	end_time := clock_timestamp();
	--epoch
	RAISE NOTICE 'crm_cust_info loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--prd info
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM 'C:/Users/yuji/OneDrive/Data warehouse project/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;
	end_time := clock_timestamp();
	RAISE NOTICE 'crm_prd_info loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--sales details
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM 'C:/Users/yuji/OneDrive/Data warehouse project/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;
	end_time := clock_timestamp();
	RAISE NOTICE 'crm_sales_details loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--erp cust az
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM 'C:/Users/yuji/OneDrive/Data warehouse project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;
	end_time := clock_timestamp();
	RAISE NOTICE 'erp_cust_az12 loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--erp loc a101
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM 'C:/Users/yuji/OneDrive/Data warehouse project/datasets/source_erp/loc_a101.csv'
    DELIMITER ','
    CSV HEADER;
	end_time := clock_timestamp();
	RAISE NOTICE 'erp_loc_a101 loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--erp px cat
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:/Users/yuji/OneDrive/Data warehouse project/datasets/source_erp/px_cat_g1v2.csv'
    DELIMITER ','
    CSV HEADER;
	end_time := clock_timestamp();
	RAISE NOTICE 'erp_px_cat_g1v2 loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	RAISE NOTICE 'All tables loaded successfully';
	batch_end_time := clock_timestamp();
	RAISE NOTICE 'Whole Batch loaded in % seconds', ROUND(EXTRACT(EPOCH FROM end_time -start_time), 2);
--try and except to catch error
EXCEPTION
	WHEN OTHERS THEN
		RAISE WARNING 'Error during bulk load: %',SQLERRM;
END;
$$;
