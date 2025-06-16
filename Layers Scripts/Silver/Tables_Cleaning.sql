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
------------------------------------------------------------------------------------------------------

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
--  'silver.crm_prd_info'
-- ====================================================================
-- ==============================================================================
-- Update our query
-- Insert based on select into in silver layer after cleaning data in above steps
-- ==============================================================================   

-- ===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
-- ===============================================================================








    


