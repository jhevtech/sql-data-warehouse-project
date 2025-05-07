

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql 
AS $$


-- Get time duration
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;

BEGIN
	batch_start_time := clock_timestamp();
	--crm cust info
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE 'Inserting Data into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)	
	SELECT 
		cst_id, 
		cst_key, 
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname, 
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
		     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		   	 ELSE 'n/a' --for null to be n/a
		END cst_marital_status, -- normalize marital status values for clarity	    
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
		     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		   	 ELSE 'n/a' 
		END cst_gndr, --normalize gender values for clarity
		cst_create_date 
	FROM (
	SELECT *, 
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t WHERE flag_last = 1; -- filtering to select the most recent record per customer
	end_time := clock_timestamp();
	RAISE NOTICE 'crm_cust_info loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--prd info
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE 'Inserting Data into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt	
	)
	SELECT 
		prd_id,
		--prd_key,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --extract category id, and making new column
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, --extract product key
		prd_nm,
		COALESCE(prd_cost:: NUMERIC, 0) AS prd_cost, --using coalesce for isnull to change null to 0	
		CASE UPPER(TRIM(prd_line))
			WHEN  'M' THEN 'Mountain' 
			WHEN  'R' THEN 'Road' 
			WHEN  'S' THEN 'Other Sales' 
			WHEN  'T' THEN 'Touring' 
			ELSE 'n/a'
		END AS prd_line, --data normalization: prd line to be more readable and map letters to words r -> road (can use this syntax when mapping)
		prd_start_dt,
		--USED THE FOLLOWING START DATE AS THE PREVOUS END DATE TO MAKE THE DATE ORDER CORRECT and calculate end date 1 day before the next start date (-1)
		CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE
			) AS prd_end_dt -- data enrichment
	FROM bronze.crm_prd_info;
	end_time := clock_timestamp();
	RAISE NOTICE 'crm_prd_info loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--sales details
	start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE 'Inserting Data into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT 
		sls_ord_num, 
		sls_prd_key, 
		sls_cust_id, 
		CASE 
			WHEN sls_order_dt = 0 or LENGTH(sls_order_dt::TEXT) != 8 THEN NULL --handling invalid data 
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) --changing data type
		END AS sls_order_dt,	
		CASE 
			WHEN sls_ship_dt = 0 or LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,	
		CASE 
			WHEN sls_due_dt = 0 or LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,	
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, --recalculating sales if original value is missing or incorrect 	
			sls_quantity, 
		CASE 
			WHEN sls_price IS NULL OR sls_price <=0 
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price -- derive price if original value is invalid
		END AS sls_price	
	FROM bronze.crm_sales_details;
	end_time := clock_timestamp();
	RAISE NOTICE 'crm_sales_details loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	-- erp cust az
	start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE 'Inserting Data into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' 
				THEN SUBSTRING(cid, 4, LENGTH(cid)) 
			ELSE cid
		END cid,-- Remove 'NAS' prefix if present
		CASE 
			WHEN bdate > CURRENT_TIMESTAMP 
				THEN NULL
			ELSE bdate
		END AS bdate,-- set future birthdates to NULL
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'FEMALE'
			WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'MALE'
			ELSE 'n/a'
		END AS gen -- normalize gender values and handle unknown cases
	FROM bronze.erp_cust_az12;
	end_time := clock_timestamp();
	RAISE NOTICE 'erp_cust_az12 loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--erp loc a101
	start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_loc_a101;
	RAISE NOTICE 'Inserting Data into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)	
	SELECT 
		REPLACE(cid, '-', ''),--replacing minus with empty string
		CASE --data normalization for country to be more consistent and remove unwanted spaces
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101;
	end_time := clock_timestamp();
	RAISE NOTICE 'erp_loc_a101 loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	--erp px cat
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	RAISE NOTICE 'Inserting Data into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)	
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	end_time := clock_timestamp();
	RAISE NOTICE 'erp_px_cat_g1v2 loaded in % seconds ', ROUND(EXTRACT(EPOCH FROM end_time - start_time), 2);

	RAISE NOTICE 'All tables loaded successfully';
	batch_end_time := clock_timestamp();
	RAISE NOTICE 'Whole batch loaded in % seconds', ROUND(EXTRACT(EPOCH FROM end_time -start_time), 2);

-- to catch error
EXCEPTION
	WHEN OTHERS THEN 
		RAISE WARNING 'Error during inserting data: %', SQLERRM;
END;
$$;

--CALL silver.load_silver();
