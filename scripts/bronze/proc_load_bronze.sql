-- CREATING A PROCEDURE TO INSERT UPDATED DATA INTO bronze.csv_customer_info

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
DECLARE @start_time DATE, @end_time DATE, @batch_start_time DATE, @batch_end_time DATE;
	BEGIN TRY 
		SET @batch_start_time = GETDATE();

		PRINT('=========================================================================');
		PRINT('Loading Bronze Layer')
		PRINT('=========================================================================');

		SET @start_time = GETDATE();

		PRINT 'Loading CSV data';

		PRINT 'Truncating table : bronze.csv_customer_info';
			TRUNCATE TABLE bronze.csv_customer_info;

		PRINT 'Inserting Data into bronze.csv_customer_info';
		BULK INSERT bronze.csv_customer_info
		FROM 'D:\dwh-ecommerce\datasets\ecommerce_customer_churn_dataset.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '--------------------------------------------';
		PRINT 'Loading Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';

	END TRY
	BEGIN CATCH 
			PRINT '=============================================';
			PRINT 'Error Occured While Loading Bronze Layer';
			PRINT 'Error Message: ' + ERROR_MESSAGE();
			PRINT '=============================================';
	END CATCH

	SET @batch_end_time = GETDATE();

	PRINT ('Entire Bronze Layer is loaded in ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds');

GO

EXEC bronze.load_bronze;
