/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.Load_Data
AS
Begin
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
    SET @batch_start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Loading Silver Layer';
    PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating tabel: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		PRINT '>> Insert data into: silver.crm_cust_info';
		insert into silver.crm_cust_info
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select 
			cst_id,
			cst_key,
			trim (cst_firstname) as cst_firstname,
			trim (cst_lastname) as cst_lastname,
			case when upper(trim(cst_marital_status)) = 'S' then 'Single'
					when upper(trim(cst_marital_status)) = 'M' then 'Married'
					else 'N/A'
				end as cst_marital_status,
			case when upper(trim(cst_gndr)) = 'F' then 'Female'
					when upper(trim(cst_gndr)) = 'M' then 'Male'
					else 'N/A'
				end as cst_gndr,
			cst_create_date
			from (
			select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as RN
			from bronze.crm_cust_info
			where cst_id is not null)
			as T 
			where rn = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating tabel: silver.crm_prd_info';
		truncate table silver.crm_prd_info; 
		PRINT '>> Insert data into: silver.crm_prd_info ';
		insert into silver.crm_prd_info 
		(   
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
			prd_id,
			Replace(SUBSTRING(prd_key,1,5), '-','_') as Cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost, 0) as prd_cost,
			Case upper(trim(prd_line))
				 when 'M' then 'Mountain' 
				 when 'R' then 'Road' 
				 when 'S' then 'Other Sales' 
				 when 'T' then 'Touring' 
				 else 'N/A '
				 end as prd_cost,
			cast(prd_start_dt as date) as prd_start_dt,
			cast(
				lead (prd_start_dt,1) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt  
		from bronze.crm_prd_info;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Loading crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating tabel: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		PRINT '>> Insert data into: silver.crm_sales_details';
		insert into silver.crm_sales_details 
		( 
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
		SELECT sls_ord_num,
		   sls_prd_key,
		   sls_cust_id,
		   case when sls_order_dt = 0 or len(sls_order_dt) <>8 then null
				else cast(cast(sls_order_dt as varchar) as date)
				end as sls_order_dt,
		   case when sls_ship_dt = 0 or len(sls_ship_dt) <>8 then null
				else cast(cast(sls_ship_dt as varchar) as date)  
				end as sls_ship_dt,
		   case when sls_due_dt = 0 or len(sls_due_dt) <>8 then null
				else cast(cast(sls_due_dt as varchar) as date)  
				end as sls_due_dt,
		   case when sls_sales <> sls_quantity * abs(sls_price) or sls_sales is null or sls_sales <= 0 
					then sls_quantity * abs(sls_price)
				else sls_sales
				end as sls_sales,
		   sls_quantity,
		   case when sls_price is null or sls_price <=0 
					then sls_sales / nullif (sls_quantity,0)
				else sls_price
				end as sls_price
		FROM bronze.crm_sales_details;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
    
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		-- Loading erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating tabel: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12 ;
		PRINT '>> Insert data into: silver.erp_cust_az12 ';
		Insert into silver.erp_cust_az12 
		(
			cid,
			bdate,gen
		)
		SELECT 
		   case when cid like 'NAS%' then substring(cid,4,len(cid)) 
				else cid 
				end as cid,
		   case when bdate > getdate() then null
				else bdate
				end as bdate,
		   case when upper(trim(gen)) in('FEMALE', 'F') then 'Female'
				when upper(trim(gen)) in('MALE', 'M') then 'Male'
				else 'N/A'
				end as gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating tabel: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		PRINT '>> Insert data into: silver.erp_loc_a101';
		Insert into silver.erp_loc_a101
		(
			cid,
			cntry
		)
		SELECT replace(cid, '-','') cid,
		   case when upper(trim(cntry)) in ('DE', 'Germany') then 'Germany'
				when upper(trim(cntry)) in ('US', 'United States','USA') then 'United States'
				when upper(trim(cntry)) = '' or cntry is null then 'N/A'
					 else trim(cntry)
				end as cntry
		FROM bronze.erp_loc_a101;
	  	SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating tabel: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		PRINT '>> Insert data into: silver.erp_px_cat_g1v2';
		Insert into silver.erp_px_cat_g1v2
		(    
			id,
		    cat,
		    subcat,
		    maintenance
		)
		SELECT id,
			  cat,
			  subcat,
			  maintenance
		FROM bronze.erp_px_cat_g1v2;
	  	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	End try

	Begin catch
	  	PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	End catch
End;

----
