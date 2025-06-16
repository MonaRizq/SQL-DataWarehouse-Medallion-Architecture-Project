/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
/*--we will make data modeling starting with creating dimentions and fact table
-- we have in silver layer 3 tables related to the customer, so we will join them togther as View
--Table silver.crm_cust_info is the master table so will join other tables with him.
--Rename columns to have readable name
--Arrange the order to make it more logical
--create surrogate key to connect data model to not depend on source system*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers
as
	select 
		row_number() over(order by cst_id) as customer_key,
		Cu.cst_id as customer_id,
		Cu.cst_key as customer_number,
		Cu.cst_firstname as first_name,
		Cu.cst_lastname as last_name ,
			Lo.cntry as country,
		Cu.cst_marital_status as marital_status,
		case when Cu.cst_gndr <> 'N/A' then Cu.cst_gndr    --CRM is the master for gender data
			else coalesce(ca.gen, 'N/A')
			end as gender,
		Ca.bdate as birthdate,
		Cu.cst_create_date as create_date
	from silver.crm_cust_info Cu
	left join silver.erp_cust_az12 Ca
		on cu.cst_key = ca.cid
	left join silver.erp_loc_a101 Lo
		on cu.cst_key = lo.cid
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
/*it is slowley changing dimension type 2 and depend on the business we will decide what we will analysis if all data or current data
-- we will analyis here the current data so will filter all current data 
--make sure no duplicate
--Sort columns and rename them*/
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
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
	where prd_end_dt is null   --filter historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
/*--Create Fact table
--check tables to join
--Make data lookup to replace business keys for customer and product with surrgoate key*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
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
GO
----------------
