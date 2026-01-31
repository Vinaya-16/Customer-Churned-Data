-- Create SQL DDL scripts for all csv files in the crm and erp systems.


IF OBJECT_ID('bronze.cust_churn_info', 'U') IS NOT NULL
	DROP TABLE bronze.cust_churn_info;

GO

CREATE TABLE bronze.cust_churn_info
(
	age								INT,
	gender							NVARCHAR(30),
	country							NVARCHAR(30),
	city							NVARCHAR(30),
	membership_years				DECIMAL(10,2),
	login_frequency					INT,
	session_duration_avg			DECIMAL(10,2),
	pages_per_session				DECIMAL(10,2),
	cart_abandonment_rate			DECIMAL(10,2),
	wishlist_items					INT,
	total_purchases					DECIMAL(10,2),
	average_order_value				DECIMAL(10,2),
	days_since_last_purchase		INT,
	discount_usage_rate				DECIMAL(10,2),
	returns_rate					DECIMAL(10,2),
	email_open_rate					DECIMAL(10,2),
	customer_service_calls			INT,
	product_reviews_written			INT,
	social_media_enagagement_score	DECIMAL(10,2),
	mobile_app_usage				DECIMAL(10,2),
	payment_method_diversity		INT,
	lifetime_value					DECIMAL(10,2),
	credit_balance					DECIMAL(10,2),
	churned							BIT,
	signup_quarter					NVARCHAR(10)
);

GO

-- Develop SQL load scripts
-- Write SQL bulk insert to load csv file into your bronze table

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
DECLARE @start_time DATE, 
		@end_time DATE, 
		@batch_start_time DATE, 
		@batch_end_time DATE;
BEGIN 
	SET @batch_start_time = GETDATE();

	BEGIN TRY 
		
		PRINT '============================================================';
		PRINT 'Loading Bronze layer';
		PRINT '============================================================';

		PRINT 'Loading CSV data';

		SET @start_time = GETDATE();

		PRINT 'Truncating table : bronze.cust_churn_info';
		TRUNCATE TABLE bronze.cust_churn_info;

		PRINT 'Inserting Data into bronze.cust_churn_info';

		BULK INSERT bronze.cust_churn_info
		FROM 'D:\Data Analyst\dwh-ecommerce\datasets\ecommerce_customer_churn_dataset.csv'
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

	PRINT 'Entire Bronze Layer is loaded in ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';

END;

GO

EXEC bronze.load_bronze;