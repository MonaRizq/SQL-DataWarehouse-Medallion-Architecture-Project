/*
===============================================================================
Tables Cleaning steps & Quality check in Silver Layer
===============================================================================
Script Purpose:
This script clrify steps followed in Data cleaning,
various quality checks for data consistency, accuracy, and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
===============================================================================
*/

-- ====================================================================
-- 'silver.crm_cust_info'
-- ====================================================================
--1.Check if ther any duplicates in data
    select cst_id, count(*)
    from bronze.crm_cust_info
    group  by cst_id
    having count(*) > 1 or cst_id is null;

    --Using row number to show duplicate records
    select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as RN
    from bronze.crm_cust_info
    where cst_id = 29466;

    --Selecting all duplicates data using row number
    select * from (select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as RN
    from bronze.crm_cust_info)
    as T 
    where rn <> 1 ;

--2.Check unwanted white splaces (expected no result)
    -- keep checking for all tables
    select *
    from bronze.crm_cust_info
    where cst_firstname <> TRIM(cst_firstname);

    --Table after removing duplicate and trim columns with white spaces
    select 
    	cst_id,
    	cst_key,
    	trim (cst_firstname) as cst_firstname,
    	trim (cst_lastname) as cst_lastname,
    	cst_marital_status,
    	cst_gndr,
    	cst_create_date
  	from (
          		select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as RN
          		from bronze.crm_cust_info)
          		as T 
  	where rn = 1;

--3.Check data consistency inside cst_marital_status and cst_gndr
    select distinct cst_id,cst_marital_status
    from bronze.crm_cust_info 
    where cst_marital_status is null;

    --Standarlize gender name inside data & trim tables incase data is not consistent in the future & handle NULL.
    select 
        cst_id,
        cst_key,
        trim (cst_firstname) as cst_firstname,
        trim (cst_lastname) as cst_lastname,
        case when upper(trim(cst_marital_status)) = 'S' then 'Single'
              when upper(trim(cst_marital_status)) = 'M' then 'Married'
              else 'N/A'
              end as cst_marital_status,
        case when upper(trim(cst_gndr)) = 'M' then 'Female'
              when upper(trim(cst_gndr)) = 'M' then 'Male'
              else 'N/A'
              end as cst_gndr,
        cst_create_date
        from (
        select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as RN
    from bronze.crm_cust_info) as T 
    where rn = 1;
--------------------------------------------------------------------------------------------
-- ==============================================================================
-- Insert based on select into in silver layer after cleaning data in above steps
-- ==============================================================================
    insert into silver.crm_cust_info(cst_id,
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
              where cst_id is not null) as T 
    		where rn = 1;
-- ===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
-- ===============================================================================

-- 1.Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
    select cst_id, count(*)
    from Silver.crm_cust_info
    group  by cst_id
    having count(*) > 1 or cst_id is null;

-- 2.Check for Unwanted Spaces
-- Expectation: No Results
    select *
    from silver.crm_cust_info
    where cst_firstname <> TRIM(cst_firstname);

-- 3.Data Standardization & Consistency
    select distinct cst_marital_status
    from silver.crm_cust_info;
    select distinct cst_gndr
    from silver.crm_cust_info;

--4.Final Check of data in Silver Layer
    select * from silver.crm_cust_info;
------------------------------------------------------------------------------------------------------

-- ====================================================================
--  'silver.crm_prd_info'
-- ====================================================================
    select * from bronze.crm_prd_info;
    
--1.Check if ther any duplicates in data 
    select prd_id, count(*)
    from bronze.crm_prd_info
    group  by prd_id
    having count(*) > 1 or prd_id is null;
    
--2.Derived new columns
    --Extract first 5 char from prd_key which reprsent category_id to can join table with bronze.erp_px_cat_g1v2 table
    select 
        prd_id,
        prd_key,
        SUBSTRING(prd_key,1,5) as Cat_id,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    from bronze.crm_prd_info;
    
    --Check category_id in bronze.erp_px_cat_g1v2 
    select distinct id
    from bronze.erp_px_cat_g1v2;
    
    --Replace - with _ in column Cat_id to match data in bronze.erp_px_cat_g1v2
    select 
        prd_id,
        prd_key,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') as Cat_id,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    from bronze.crm_prd_info;
    
    --Check if any category is not exist in other table
    select 
        prd_id,
        prd_key,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') as Cat_id,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    from bronze.crm_prd_info
    WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_')
    not in (select distinct id from bronze.erp_px_cat_g1v2);
    
    --Extract next part from prd_key which reprsent Location_id to can join table with bronze.crm_sales_details table
    select 
        prd_id,
        prd_key,
        SUBSTRING(prd_key,1,5) as Cat_id,
        SUBSTRING(prd_key,7,len(prd_key)) as prd_key, --Dynamic Extraction
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    from bronze.crm_prd_info
    where SUBSTRING(prd_key,7,len(prd_key))
    not in (select distinct sls_prd_key from bronze.crm_sales_details); --we can use it to check if there is any match to could make join
    
    --Check unwanted white splaces
    --expected no result
    -- keep checking for all tables
    select *
    from bronze.crm_prd_info
    where prd_nm <> TRIM(prd_nm);
    
    select * from bronze.crm_prd_info;
    
    --Check quality of numbers in prd_cost nulls & negative numbers
    select prd_cost
    from bronze.crm_prd_info
    where prd_cost is null or prd_cost < 0;
    
    --Replace null with zero
    select 
        prd_id,
        prd_key,
        SUBSTRING(prd_key,1,5) as Cat_id,
        SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
        prd_nm,
        isnull(prd_cost, 0) as prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    from bronze.crm_prd_info;
    
--3.Check data consistency inside prd_line 
    --Note will ask business for meaning of abbreviation
    select distinct prd_line
    from bronze.crm_prd_info;
    
    select 
        prd_id,
        prd_key,
        SUBSTRING(prd_key,1,5) as Cat_id,
        SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
        prd_nm,
        isnull(prd_cost, 0) as prd_cost,
        Case upper(trim(prd_line))  --Reshape case incase has simple value
             when 'M' then 'Mountain' 
             when 'R' then 'Road' 
             when 'S' then 'Other Sales' 
             when 'T' then 'Touring' 
             else 'N/A '
             end as prd_cost,
        prd_start_dt,
        prd_end_dt
    from bronze.crm_prd_info;
    
--4.Check for invalid date orders
    select * from bronze.crm_prd_info 
    where prd_end_dt < prd_start_dt;
    
    --after inserting data in excel and make validation we will use only start date to get end date using lead function
    --as end date is smaller than stat date which is not valid
    select 
        prd_id,
        prd_key,
        prd_start_dt,
        lead (prd_start_dt,1) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
    from bronze.crm_prd_info
    where prd_key in ( 'CL-CA-CA-1098', 'CO-RF-FR-R92R-62');
    
    select distinct prd_line
    from bronze.crm_prd_info;
-- ==============================================================================
-- Update our query
-- Insert based on select into in silver layer after cleaning data in above steps
-- ==============================================================================    
    truncate table silver.crm_prd_info; 
    insert into silver.crm_prd_info 
    (   prd_id,
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
            lead (prd_start_dt,1) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt  --Data enrichment (adding value to enhance our dataset for analysis data)
    from bronze.crm_prd_info;
-- ===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
-- ===============================================================================

  select * from silver.crm_prd_info;
--1.Check if any duplicate
    select prd_id, count(*)
    from silver.crm_prd_info
    group  by prd_id
    having count(*) > 1 or prd_id is null;

--2.Check white spaces
    select *
    from silver.crm_prd_info
    where prd_nm <> TRIM(prd_nm);

--3.Check quality of numbers in prd_cost nulls & negative numbers
select prd_cost
from silver.crm_prd_info
where prd_cost is null or prd_cost < 0;
---------------------------------------------------------------------------------------------------------------------

-- ====================================================================
--  'silver.crm_sales_details'
-- ====================================================================
--we will not check duplicates in data as this tables has measure and normally will have duplicates
    SELECT sls_ord_num,
              sls_prd_key,
              sls_cust_id,
              sls_order_dt,
              sls_ship_dt,
              sls_due_dt,
              sls_sales,
              sls_quantity,
              sls_price
        FROM bronze.crm_sales_details;

--1.check white spaces in text column
    SELECT sls_ord_num
    FROM bronze.crm_sales_details
    where sls_ord_num <> trim(sls_ord_num);

    select * from Silver.crm_cust_info;

--2.Check if any prd key not exist on product table (refrenetial integrity)
    SELECT sls_ord_num,
          sls_prd_key,
          sls_cust_id,
          sls_order_dt,
          sls_ship_dt,
          sls_due_dt,
          sls_sales,
          sls_quantity,
          sls_price
    FROM bronze.crm_sales_details
    where sls_prd_key not in (select prd_key from silver.crm_prd_info);

    --same for customer 
    SELECT sls_ord_num,
          sls_prd_key,
          sls_cust_id,
          sls_order_dt,
          sls_ship_dt,
          sls_due_dt,
          sls_sales,
          sls_quantity,
          sls_price
    FROM bronze.crm_sales_details
    where sls_cust_id not in (select cst_id from silver.crm_cust_info);

--3.change date tables from int to dates
    --first will check if there any zeros and make it null
    select 
    nullif(sls_order_dt,0)
    from bronze.crm_sales_details
    where sls_order_dt <= 0;

    --check len of charchters as it should be 8 num to could convert it to date
    select 
    nullif(sls_order_dt,0)
    from bronze.crm_sales_details
    where len(sls_order_dt) <> 8;

    --check max value to make sure of date boundries
    select 
    nullif(sls_order_dt,0)
    from bronze.crm_sales_details
    where sls_order_dt > 20500101;


    SELECT sls_ord_num,
          sls_prd_key,
          sls_cust_id,
    	  case when sls_order_dt = 0 or len(sls_order_dt) <>8 then null
    			else cast(cast(sls_order_dt as varchar) as date)  --we cant change directlry from int to date we should change to string first
    			end as sls_order_dt,
    	  case when sls_ship_dt = 0 or len(sls_ship_dt) <>8 then null
    			else cast(cast(sls_ship_dt as varchar) as date)  
    			end as sls_ship_dt,
    	  case when sls_due_dt = 0 or len(sls_due_dt) <>8 then null
    			else cast(cast(sls_due_dt as varchar) as date)  
    			end as sls_due_dt,
          sls_sales,
          sls_quantity,
          sls_price
    FROM bronze.crm_sales_details;

    --will do same steps for the other 2 columns
    select 
    nullif(sls_ship_dt,0)
    from bronze.crm_sales_details
    where len(sls_ship_dt) <> 8
    or sls_ship_dt <=0
    or sls_ship_dt > 20500101 
    or sls_ship_dt < 19000101;
    -- no issue found but will apply same rules incase any issue in the future

    select 
    nullif(sls_due_dt,0)
    from bronze.crm_sales_details
    where len(sls_due_dt) <> 8
    or sls_due_dt <=0
    or sls_due_dt > 20500101 
    or sls_due_dt < 19000101;

--4.check for invalid date orders
    select *
    from bronze.crm_sales_details
    where sls_order_dt > sls_due_dt or sls_order_dt > sls_ship_dt;

    --apply below business rules
    --1. sum sales = Q * price
    --2. no negative, nulls or zeros
    SELECT
          sls_sales as old_sls_sales,
          sls_quantity,
          sls_price as old_sls_price, 
    	  case when sls_sales <> sls_quantity * abs(sls_price) or sls_sales is null or sls_sales <= 0 
    				then sls_quantity * abs(sls_price)  --recalculate sls_sales if any issue in data
    			    else sls_sales
    			    end as sls_sales,
    	  case when sls_price is null or sls_price <=0 
    				then sls_sales / nullif (sls_quantity,0)
    			    else sls_price                         --recalculate sls_sales if any issue in data
    			    end as sls_price
    FROM bronze.crm_sales_details
    where sls_sales <> sls_quantity * sls_price
    or sls_sales is null or sls_quantity is null or sls_price is null
    or sls_sales <= 0 or sls_quantity <= 0  or sls_price <= 0 
    order by old_sls_sales ,sls_quantity ;
-- ==============================================================================
-- Update our query
-- Insert based on select into in silver layer after cleaning data in above steps
-- ==============================================================================   
    insert into silver.crm_sales_details 
        ( sls_ord_num,
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
-- ===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
-- ===============================================================================
    select * from silver.crm_sales_details;
    
    SELECT
          sls_sales as sls_sales,
          sls_quantity,
          sls_price 
    FROM silver.crm_sales_details
    where sls_sales <> sls_quantity * sls_price
    or sls_sales is null or sls_quantity is null or sls_price is null
    or sls_sales <= 0 or sls_quantity <= 0  or sls_price <= 0 
    order by sls_sales ,sls_quantity ;

------------------------------------------------------------------------------------------------------------
-- ====================================================================
-- 'silver.erp_cust_az12'
-- we will connect this table with crm_cust_info so we should make sure cst_key and cid in 2 tables are matched
-- ====================================================================
--1.we will find there is char should be removed from cid in some rows
    SELECT cid,
          bdate,
          gen
    FROM bronze.erp_cust_az12;
    
    select cst_key from silver.crm_cust_info;

    --Will remove extra char from cid column
    SELECT cid,
    	   case when cid like 'NAS%' then substring(cid,4,len(cid)) 
    	        else cid 
    	        end as cid,
           bdate,
           gen
    FROM bronze.erp_cust_az12;

    --check after removing if there any remaining unwanted char
    SELECT 
    	   case when cid like 'NAS%' then substring(cid,4,len(cid)) 
    	        else cid 
    	        end as cid,
           bdate,
           gen
    FROM bronze.erp_cust_az12
    where case when cid like 'NAS%' then substring(cid,4,len(cid)) 
    	        else cid 
    	        end not in (select distinct cst_key from silver.crm_cust_info);

--2.check bdate if we have something out of date
    SELECT distinct bdate
    FROM bronze.erp_cust_az12
    where bdate < '1924-01-01' or bdate > getdate();

    --will replace data that we 100% sure is not correct which is future date
    SELECT 
    	   case when cid like 'NAS%' then substring(cid,4,len(cid)) 
    	        else cid 
    	        end as cid,
    	   case when bdate > getdate() then null
    	        else bdate
    			end as bdate,
           gen
    FROM bronze.erp_cust_az12;

--3.check gender consistency 
    select distinct gen
    from bronze.erp_cust_az12;

--update column and make it constant
    SELECT 
    	   case when cid like 'NAS%' then substring(cid,4,len(cid)) 
    	        else cid 
    	        end as cid,
    	   case when bdate > getdate() then null
    	        else bdate
    			end as bdate,
    	   case when upper(trim(gen)) in('FEMALE', 'F') then 'Female'
    			when upper(trim(gen)) in('MALE', 'M') then 'Male'
    			else gen
    			end as gen
    FROM bronze.erp_cust_az12;

--Check after update
    select distinct gen as old_gen,
    	   case when upper(trim(gen)) in('FEMALE', 'F') then 'Female'
    			when upper(trim(gen)) in('MALE', 'M') then 'Male'
    			else 'N/A'
    			end as gen
    from bronze.erp_cust_az12;
-- ==============================================================================
-- Update our query
-- Insert based on select into in silver layer after cleaning data in above steps
-- ==============================================================================  
    Insert into silver.erp_cust_az12 (cid,bdate,gen)
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
-- ===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
-- ===============================================================================
--1.Check data qulaity 
    SELECT 
    	   cid
           bdate,
           gen
    FROM silver.erp_cust_az12
    where cid not in  (select distinct cst_key from silver.crm_cust_info);

-- 2.Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
    SELECT distinct bdate
    FROM silver.erp_cust_az12
    where bdate > getdate();

-- 3.Data Standardization & Consistency
select distinct gen
from silver.erp_cust_az12;

select * from silver.erp_cust_az12;

------------------------------------------------------------------------------------------------------------
-- ====================================================================
--  'silver.erp_loc_a101'
-- we will connect this table with crm_cust_info so we should make sure cst_key and cid in 2 tables are matched
-- ====================================================================
    SELECT cid,
           cntry
      FROM bronze.erp_loc_a101;
    
    select cst_key from silver.crm_cust_info;

--1.Replace unmatched atring
    SELECT replace(cid, '-','') cid,
           cntry
      FROM bronze.erp_loc_a101;

    --Make sure they are matched
    SELECT replace(cid, '-','') cid,
           cntry
    FROM bronze.erp_loc_a101
    where replace(cid, '-','') not in (select cst_key from silver.crm_cust_info);

--2.Check data consistency in cntry
    select distinct cntry
    from bronze.erp_loc_a101;

    --Make data consistent
    select distinct cntry as old_cntry,
    	   case when upper(trim(cntry)) in ('DE', 'Germany') then 'Germany'
    			when upper(trim(cntry)) in ('US', 'United States','USA') then 'United States'
    			when upper(trim(cntry)) = '' or cntry is null then 'N/A'
    				 else trim(cntry)
    			     end as cntry
    from bronze.erp_loc_a101
    order by cntry;
-- ==============================================================================
-- Update our query
-- Insert based on select into in silver layer after cleaning data in above steps
-- ==============================================================================   
    Insert into silver.erp_loc_a101 (cid, cntry)
    SELECT replace(cid, '-','') cid,
    	   case when upper(trim(cntry)) in ('DE', 'Germany') then 'Germany'
    			when upper(trim(cntry)) in ('US', 'United States','USA') then 'United States'
    			when upper(trim(cntry)) = '' or cntry is null then 'N/A'
    				 else trim(cntry)
    			end as cntry
      FROM bronze.erp_loc_a101;
-- ===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
-- ===============================================================================
    SELECT cid,
           cntry
     FROM silver.erp_loc_a101;
    select cst_key from silver.crm_cust_info;

-- Data Standardization & Consistency
    select distinct cntry
    from silver.erp_loc_a101
    order by cntry;
    
    select * from silver.erp_loc_a101;

------------------------------------------------------------------------------------------------------------
-- ====================================================================
    --  'silver.erp_px_cat_g1v2'
    --   Make sure that we can connect table with silver.crm_prd_info
-- ====================================================================
    SELECT id,
          cat,
          subcat,
          maintenance
      FROM bronze.erp_px_cat_g1v2;
    
    select * 
    from silver.crm_prd_info;

--1.Check unwanted spaces in other columns
    select *
    from bronze.erp_px_cat_g1v2
    where maintenance <> trim(maintenance);

--2.Check data constinecty 
    select distinct cat
    from bronze.erp_px_cat_g1v2;
    
    select distinct maintenance
    from bronze.erp_px_cat_g1v2;

    select distinct subcat
    from bronze.erp_px_cat_g1v2;
-- ==============================================================================
-- Update our query
-- Insert based on select into in silver layer after cleaning data in above steps
-- ==============================================================================   
--No issue found so will insert data into column in silver 
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
-- ===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
-- ===============================================================================
    select * from silver.erp_px_cat_g1v2;
------------------------------------------------------------------------------------------------------------
