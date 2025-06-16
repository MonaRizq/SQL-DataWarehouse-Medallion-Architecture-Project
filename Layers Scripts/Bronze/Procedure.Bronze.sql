/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
Create or Alter procedure bronze.load_bronze as
Begin
	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime,@batch_end_time datetime
	Begin try
		Print '===================================================';
		Print 'Loading Bronze Layer';
		Print '===================================================';

		Print '---------------------------------------------------';
		Print 'Loading CRM Tables';
		Print '---------------------------------------------------';

		set @batch_start_time = GETDATE();
		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_cust_info';
		Truncate Table bronze.crm_cust_info;
		Bulk Insert bronze.crm_cust_info
		from 'D:\Data\Data Engineer\Baraa\DataDarehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';
		print '>> -------------------';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info;
		Bulk Insert bronze.crm_prd_info
		from 'D:\Data\Data Engineer\Baraa\DataDarehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';
		print '>> -------------------';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_sales_details';
		Truncate Table bronze.crm_sales_details;
		Bulk Insert bronze.crm_sales_details
		from 'D:\Data\Data Engineer\Baraa\DataDarehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';
		print '>> -------------------';

		Print '---------------------------------------------------';
		Print 'Loading ERP Tables';
		Print '---------------------------------------------------';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12;
		Bulk Insert bronze.erp_cust_az12
		from 'D:\Data\Data Engineer\Baraa\DataDarehouse Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';
		print '>> -------------------';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101;
		Bulk Insert bronze.erp_loc_a101
		from 'D:\Data\Data Engineer\Baraa\DataDarehouse Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';
		print '>> -------------------';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2;
		Bulk Insert bronze.erp_px_cat_g1v2
		from 'D:\Data\Data Engineer\Baraa\DataDarehouse Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';
		print '>> -------------------';
		
		set @batch_end_time = GETDATE();
		print '======================================================='
		print 'Loading Bronze Layer is Completed';
		print 'Total Load Duration ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' Second';
		print '======================================================='
	End try
	Begin Catch
		print '======================================================='
		print 'Error occured during loading bronze layer'
		print 'Error Message' + error_message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state()as nvarchar);
		print '======================================================='
	End Catch
End;

--------------------------------------------------------------------------------------------------------
