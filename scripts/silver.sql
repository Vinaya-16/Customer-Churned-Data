
-- ================================================================
-- CLEAN AND LOAD cust_churn_info INTO SILVER LAYER
-- ================================================================

-- CREATE A PROCEDURE TO LOAD CLEAN DATA INTO SILVER DATABASE IN RESPECTIVE TABLES

CREATE OR ALTER PROCEDURE silver.load_silver_cust_churn_info AS
DECLARE @start_time DATE, @end_time DATE, @batch_start_time DATE, @batch_end_time DATE;
BEGIN 
    
    BEGIN TRY
     
        SET @batch_start_time = GETDATE();

        SET @start_time = GETDATE();
        
        -- TRUNCATING THE TABLE
        PRINT('Truncating Data Into silver.cust_churn_info')
        TRUNCATE TABLE silver.cust_churn_info;

        PRINT('Inserting Data Into silver.cust_churn_info')

        INSERT INTO silver.cust_churn_info
        (
            age,
            gender,
            country,
            city,
            membership_years,
            login_frequency,
            session_duration_avg,
            pages_per_session,
            cart_abandonment_rate,
            wishlist_items,
            total_purchases,
            average_order_value,
            days_since_last_purchase,
            discount_usage_rate,
            returns_rate,
            email_open_rate,
            customer_service_calls,
            product_reviews_written,
            social_media_enagagement_score,
            mobile_app_usage,
            payment_method_diversity,
            lifetime_value,
            credit_balance,
            churned,
            signup_quarter
        )
        SELECT
            CASE WHEN age < 18 OR age > 95 THEN NULL ELSE age END,
            NULLIF(TRIM(gender), ''),
            NULLIF(TRIM(country), ''),
            NULLIF(TRIM(city), ''),
            membership_years,
            login_frequency,
            session_duration_avg,
            pages_per_session,
            cart_abandonment_rate,
            wishlist_items,
            total_purchases,
            average_order_value,
            days_since_last_purchase,
            discount_usage_rate,
            returns_rate,
            email_open_rate,
            customer_service_calls,
            product_reviews_written,
            social_media_enagagement_score,
            mobile_app_usage,
            payment_method_diversity,
            lifetime_value,
            credit_balance,
            churned,
            CASE 
                WHEN signup_quarter IN ('Q1','Q2','Q3','Q4') THEN signup_quarter
                ELSE NULL
            END
        FROM bronze.cust_churn_info;

        SET @end_time = GETDATE();

		PRINT '--------------------------------------------';
		PRINT 'Loading Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';

    END TRY 
    BEGIN CATCH
		PRINT '=============================================';
		PRINT 'Error Occured While Loading Bronze Layer';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT '=============================================';
    END CATCH
    SET @batch_end_time = GETDATE();

	PRINT 'Entire Sillver Layer is loaded in ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';

END;

GO

EXEC silver.load_silver