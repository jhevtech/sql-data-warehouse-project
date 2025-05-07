/*
###########################################################################################
Quality Checks
###########################################################################################
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, 
  and standardization across the 'silver' schema. It includes checks for:
  - NUll or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes: 
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
###########################################################################################
*/

-- ########################################################################################
-- Checking 'silver.crm_cust_info'
-- ########################################################################################
-- Checking for NULLS or Duplicates in Primary Key
-- Expectation: No results
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
--expectation: no results
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- data standardization & consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- ########################################################################################
-- Checking 'silver.crm_prd_info'
-- ########################################################################################
--Check for nulls or duplicates in primary key
--Expectation: no result
SELECT prd_id, COUNT(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
--expectation: no results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or Negatice numbers
-- expectation: no results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT *
FROM silver.crm_prd_info

-- ########################################################################################
-- Checking 'silver.crm_sales_details'
-- ########################################################################################
--check for invalid dates
SELECT  
	NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt:: TEXT) != 8 

-- check for invalid date orders
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt

--check data consistency: between sales, quantity, and price
-- > sales = quantity * price
-- > values must not be NULL, Zero or negative.
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price 
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price

-- check for invalid date orders in the updated table
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt

--checking updated table to see if there is any error
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details

-- ########################################################################################
-- Checking 'silver.erp_cust_az12'
-- ########################################################################################
-- looking how tables relate to each other and making them similar by removing nas 
-- then checking for any unmatched data from both tables
SELECT 
	cid,
	CASE 
		WHEN cid LIKE 'NAS%' 
			THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- to check any unmatching data		
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- identify out of ranger dates
SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_TIMESTAMP() --check for birthdays in future/PAST
--script to make date null if it is in the future
SELECT DISTINCT
	CASE 
		WHEN bdate > CURRENT_TIMESTAMP 
			THEN NULL
		ELSE bdate
	END AS bdate
FROM bronze.erp_cust_az12

--data normalization/standardization & consistency 
--(check to see if information is consistent, is not [null, f, m, ,male, female] so time to standardize it)
SELECT DISTINCT 
	gen, --run by self to check 
	CASE --is not after running so standardize using case when
		WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'FEMALE'
		WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'MALE'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12

-- ########################################################################################
-- Checking 'silver.erp_loc_a101'
-- ########################################################################################
-- change the - to nothing to match crm cust info key id
SELECT 
	REPLACE(cid,'-',''),
	cntry
FROM bronze.erp_loc_a101 
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info) --check

-- data standardization & consistency 
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT 
	REPLACE(cid,'-',''),
	cntry AS old_cntry,
	CASE --DATA normalization for country to be more consistent
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101 

--testing final results
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry
SELECT * FROM silver.erp_loc_a101

-- ########################################################################################
-- Checking 'silver.erp_px_cat_g1v2'
-- ########################################################################################
--check for unwanted spaces
-- expectation: nothing
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- data standardization & consistency
SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2






