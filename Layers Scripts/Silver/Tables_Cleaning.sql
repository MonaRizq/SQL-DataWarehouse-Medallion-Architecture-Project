/*
===============================================================================
Tables Cleaning steps & Quality check in Silver Layer
===============================================================================
Script Purpose:
    This script clrify steps followed in Data cleaning and various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
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
--Insert based on select into new table in silver layer after cleaning data in above steps
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
/*
===============================================================================
-- Checking data quality after Data Cleaning and Loading it into silver layer 
-- Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

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

