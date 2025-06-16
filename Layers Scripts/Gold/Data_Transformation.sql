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

-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
    SELECT 
        customer_key,
        COUNT(*) AS duplicate_count
    FROM gold.dim_customers
    GROUP BY customer_key
    HAVING COUNT(*) > 1;
------------------------------------------------------------------------------------------------------
--===============================================================================
  --Create product Dimenstion
  --it is slowley changing dimension
--===============================================================================
    select * from silver.crm_prd_info;
    select * from silver.erp_px_cat_g1v2;

--1.we will analysis here the current data so will filter all historical data and that is depend on the business requirments
--2..make sure no duplicate
    select prd_id, count(*)
    from (
    select 
    	pr.prd_id,
    	pr.cat_id,
    	pr.prd_key,
    	pr.prd_nm,
    	pr.prd_cost,
    	pr.prd_line,
    	pr.prd_start_dt,
    	ca.cat,
    	ca.subcat,
    	ca.maintenance
    from silver.crm_prd_info pr
    left join silver.erp_px_cat_g1v2 ca
    	 on pr.cat_id = ca.id
    where prd_end_dt is null) t                --filter historical data
    group by prd_id
    having count(*) > 1;

--3.Sort columns and rename them
    select 
    	pr.prd_id as product_id,
    	pr.prd_key as product_number,
    	pr.prd_nm as product_name,
    	pr.cat_id as category_id,
    	ca.cat as category,
    	ca.subcat as subcategory,
    	ca.maintenance,
    	pr.prd_cost as cost,
    	pr.prd_line as line,
    	pr.prd_start_dt as start_date
    from silver.crm_prd_info pr
    left join silver.erp_px_cat_g1v2 ca
    	 on pr.cat_id = ca.id
    where prd_end_dt is null

--Create view dimenstion and surrgoate key
    create view gold.dim_products
    as
    select 
    	ROW_NUMBER() over(order by pr.prd_start_dt, pr.prd_key) as product_key,
    	pr.prd_id as product_id,
    	pr.prd_key as product_number,
    	pr.prd_nm as product_name,
    	pr.cat_id as category_id,
    	ca.cat as category,
    	ca.subcat as subcategory,
    	ca.maintenance,
    	pr.prd_cost as cost,
    	pr.prd_line as line,
    	pr.prd_start_dt as start_date
    from silver.crm_prd_info pr
    left join silver.erp_px_cat_g1v2 ca
    	 on pr.cat_id = ca.id
    where prd_end_dt is null

    select * from gold.dim_products
        
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
    SELECT 
        product_key,
        COUNT(*) AS duplicate_count
    FROM gold.dim_products
    GROUP BY product_key
    HAVING COUNT(*) > 1;
--------------------------------------------------------------------------------------------------------------
-- ====================================================================
-- Create fact sales
-- ====================================================================

--1.check tables that we will join with fact_sales and replace business key with surrogate key
    select * from silver.crm_sales_details;
    select * from gold.dim_products;
    
    select * from silver.crm_sales_details;
    select * from gold.dim_customers;

--Make data lookup to replace business keys for customer and product with surrgoate key
    Create view gold.fact_sales
    as
    select 
    	s.sls_ord_num as order_number,
    	pr.product_key,
    	cu.customer_key,
    	s.sls_order_dt as order_date,
    	s.sls_ship_dt as ship_date,
    	s.sls_due_dt as due_date,
    	s.sls_sales as sales_amount,
    	s.sls_quantity as quantity,
    	s.sls_price as price
    from silver.crm_sales_details s
    left join gold.dim_products pr
    	on s.sls_prd_key = pr.product_number
    left join gold.dim_customers cu
    	on s.sls_cust_id = cu.customer_id
        
 -- Check the data model connectivity between fact and dimensions
    SELECT * 
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
    LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
    WHERE p.product_key IS NULL OR c.customer_key IS NULL  
    --------------------------------------------------------------------------------------------
   --Check data in all views
    select * from gold.fact_sales;
    select * from gold.dim_customers
    select * from gold.dim_products
  --------------------------------------------------------------------------------------------


