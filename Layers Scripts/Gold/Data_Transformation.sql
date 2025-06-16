/*
===============================================================================
Script Purpose:
    This script clarify followed steps to transform data and create gold layer
    performs quality checks to validate the integrity, consistency, and accuracy of the Gold Layer. These checks ensure:
      - Uniqueness of surrogate keys in dimension tables.
      - Referential integrity between fact and dimension tables.
      - Validation of relationships in the data model for analytical purposes.
===============================================================================
  -In this layer we will make data modeling starting with creating dimentions and fact table
  - There is 3 tables related to the customer in silver layer, so we will join them togther as View
  -Table silver.crm_cust_info is the master table so will join other tables with him.
===============================================================================
*/

--===============================================================================
  --Create customer Dimenstion
--===============================================================================
    select * from silver.crm_cust_info;
    select * from silver.erp_cust_az12;
    
    select * from silver.crm_cust_info;
    select * from silver.erp_loc_a101;

--1.Join the 3 tables
    -- Also make sure that there is no duplicate keys
    select cst_id, count(*)
    from (
    select 		
    	Cu.cst_id,
    	Cu.cst_key,
    	Cu.cst_firstname,
    	Cu.cst_lastname,
    	Cu.cst_marital_status,
    	Cu.cst_gndr,
    	Cu.cst_create_date,
    	Ca.bdate,
    	Ca.gen,
    	Lo.cntry
    from silver.crm_cust_info Cu
    left join silver.erp_cust_az12 Ca
    	on cu.cst_key = ca.cid
    left join silver.erp_loc_a101 Lo
    	on cu.cst_key = lo.cid
    ) t
    group by cst_id
    having count(*) > 1;

--2. fix Data integration issue as we have 2 tables for gender
    -- Note null in second table becasue of join 
    --we will find opposite gender for same customer, so will ask business which table is the master and consider it's value.
    select 	distinct	
    	Cu.cst_gndr,
    	Ca.gen,
    	case when Cu.cst_gndr <> 'N/A' then Cu.cst_gndr      --CRM is the master for gender data
    		else coalesce(ca.gen, 'N/A')
    		end as new_gen
    from silver.crm_cust_info Cu
    left join silver.erp_cust_az12 Ca
    	on cu.cst_key = ca.cid
    left join silver.erp_loc_a101 Lo
    	on cu.cst_key = lo.cid
    order by 1,2;

--3.Rename columns to have readable name & Arrange the order of coulmns to make it more logical
    select 		
    	Cu.cst_id as customer_id,
    	Cu.cst_key as customer_number,
    	Cu.cst_firstname as first_name,
    	Cu.cst_lastname as last_name ,
    		Lo.cntry as country,
    	Cu.cst_marital_status as marital_status,
    	case when Cu.cst_gndr <> 'N/A' then Cu.cst_gndr    
    		else coalesce(ca.gen, 'N/A')
    		end as gender,
    	Ca.bdate as birthdate,
    	Cu.cst_create_date as create_date
    from silver.crm_cust_info Cu
    left join silver.erp_cust_az12 Ca
    	on cu.cst_key = ca.cid
    left join silver.erp_loc_a101 Lo
    	on cu.cst_key = lo.cid

--4.create surrogate key to connect data model to not depend on source system using window function
    create view gold.dim_customers
    as
    select 
    	row_number() over(order by cst_id) as customer_key,  -- create surrogate key using window function
    	Cu.cst_id as customer_id,
    	Cu.cst_key as customer_number,
    	Cu.cst_firstname as first_name,
    	Cu.cst_lastname as last_name ,
    		Lo.cntry as country,
    	Cu.cst_marital_status as marital_status,
    	case when Cu.cst_gndr <> 'N/A' then Cu.cst_gndr   
    		else coalesce(ca.gen, 'N/A')
    		end as gender,
    	Ca.bdate as birthdate,
    	Cu.cst_create_date as create_date
    from silver.crm_cust_info Cu
    left join silver.erp_cust_az12 Ca
    	on cu.cst_key = ca.cid
    left join silver.erp_loc_a101 Lo
    	on cu.cst_key = lo.cid
    --
    select * from gold.dim_customers
    select distinct gender from gold.dim_customers;


