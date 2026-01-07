
-- CREATE A PROCEDURE TO INSERT CLEANED DATA IN SILVER LAYER

CREATE OR ALTER PROCEDURE silver.proc_load_silver AS
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN 
    BEGIN TRY
        SET @batch_start_time = GETDATE();

	    /*
	    ==============================================================================
	    CLEAN AND LOAD THE DATA INTO silver.csv_customer_info
	    ==============================================================================
	    */
		    SET @start_time = GETDATE();

	    -- TRUNCATING TABLLE
		TRUNCATE TABLE silver.csv_customer_info;
        PRINT('Truncating Data From the Table silver.csv_customer_info');

		PRINT('Inserting Data Into Table silver.csv_customer_info');

		-- INSERTING CLEAN DATA INTO silver.csv_customer_info

        INSERT INTO silver.csv_customer_info
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
            avg_order_value,
            days_since_last_purchase,
            discount_usage_rate,
            returns_rate,
            email_open_rate,
            customer_service_calls,
            product_reviews_written,
            social_media_engagement_score,
            mobile_app_usage,
            payment_method_diversity,
            lifetime_value,
            credit_balance,
            churned,
            signup_quarter_num,
            signup_quarter_label
        )
        SELECT
            -- Age: Validate reasonable range
            CASE
                WHEN TRY_CAST(age AS DECIMAL(5,2)) BETWEEN 18 AND 100
                    THEN CAST(ROUND(TRY_CAST(age AS DECIMAL(5,2)), 0) AS INT)
                ELSE NULL
            END AS age,

            -- Gender: Standardize values
            CASE
                WHEN UPPER(TRIM(gender)) IN ('MALE','FEMALE','OTHER')
                    THEN TRIM(gender)
                WHEN UPPER(TRIM(gender)) = '' THEN NULL
                ELSE 'n/a'
            END AS gender,

            -- Location: Clean and validate
            TRIM(NULLIF(country, '')) AS country,
            TRIM(NULLIF(city, '')) AS city,

            -- Membership Years: Validate reasonable range
            CASE
                WHEN TRY_CAST(TRIM(membership_years) AS DECIMAL(5,2)) BETWEEN 0 AND 50
                    THEN TRY_CAST(TRIM(membership_years) AS DECIMAL(5,2))
                ELSE NULL
            END AS membership_years,

            -- Login Frequency: Non-negative
            CASE
                WHEN (FLOOR(TRY_CAST(TRIM(login_frequency) AS DECIMAL(10,2)))) >= 0 
                    THEN (FLOOR(TRY_CAST(TRIM(login_frequency) AS DECIMAL(10,2))))
                ELSE NULL
            END AS login_frequency,

            -- Session Duration: Non-negative, reasonable max (minutes)
            CASE
                WHEN TRY_CAST(TRIM(Session_Duration_Avg) AS DECIMAL(6,2)) BETWEEN 0 AND 1440
                    THEN TRY_CAST(TRIM(Session_Duration_Avg) AS DECIMAL(6,2))
                ELSE NULL
            END AS session_duration_avg,

            -- Pages per Session: Non-negative
            CASE
                WHEN TRY_CAST(TRIM(pages_per_session) AS DECIMAL(6,2)) >= 0
                    THEN TRY_CAST(TRIM(pages_per_session) AS DECIMAL(6,2))
                ELSE NULL
            END AS pages_per_session,

            -- Cart Abandonment Rate: Percentage (0-100)
            CASE
                WHEN TRY_CAST(TRIM(Cart_Abandonment_Rate) AS DECIMAL(5,2)) BETWEEN 0 AND 100
                    THEN TRY_CAST(TRIM(Cart_Abandonment_Rate) AS DECIMAL(5,2))
                ELSE NULL
            END AS cart_abandonment_rate,

            -- Wishlist Items: Non-negative integer
            CASE
                WHEN TRY_CAST(FLOOR(TRY_CAST(TRIM(wishlist_items) AS DECIMAL(10,2))) AS INT) >= 0
                    THEN TRY_CAST(FLOOR(TRY_CAST(TRIM(wishlist_items) AS DECIMAL(10,2))) AS INT)
                ELSE NULL
            END AS wishlist_items,

             -- Total Purchases: Non-negative integer
             CASE
                WHEN TRY_CAST(FLOOR(TRY_CAST(TRIM(total_purchases) AS DECIMAL(10,2))) AS INT) >= 0
                    THEN TRY_CAST(FLOOR(TRY_CAST(TRIM(total_purchases) AS DECIMAL(10,2))) AS INT)
                ELSE NULL
             END AS total_purchases,

             -- Average Order Value: Non-negative
             CASE
                WHEN TRY_CAST(TRIM(avg_order_value) AS DECIMAL(10,2)) >= 0
                    THEN TRY_CAST(TRIM(avg_order_value) AS DECIMAL(10,2))
                ELSE NULL
             END AS avg_order_value,

             -- Days Since Last Purchase: Non-negative
             CASE
                WHEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Days_Since_Last_Purchase) AS DECIMAL(10,2))) AS INT) >= 0
                    THEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Days_Since_Last_Purchase) AS DECIMAL(10,2))) AS INT)
                ELSE NULL
             END AS days_since_last_purchase,

            -- Discount Usage Rate: Percentage (0-100)
            CASE
                WHEN TRY_CAST(TRIM(Discount_Usage_Rate) AS DECIMAL(5,2)) BETWEEN 0 AND 100
                    THEN TRY_CAST(TRIM(Discount_Usage_Rate) AS DECIMAL(5,2))
                ELSE NULL
            END AS discount_usage_rate,

            -- Returns Rate: Percentage (0-100)
            CASE
                WHEN TRY_CAST(TRIM(returns_rate) AS DECIMAL(5,2)) BETWEEN 0 AND 100
                    THEN TRY_CAST(TRIM(returns_rate) AS DECIMAL(5,2))
                ELSE NULL
            END AS returns_rate,

            -- Email Open Rate: Percentage (0-100)
            CASE
                WHEN TRY_CAST(TRIM(Email_Open_Rate) AS DECIMAL(5,2)) BETWEEN 0 AND 100
                    THEN TRY_CAST(TRIM(Email_Open_Rate) AS DECIMAL(5,2))
                ELSE NULL
            END AS email_open_rate,

            -- Customer Service Calls: Non-negative integer
            CASE
                WHEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Customer_Service_Calls) AS DECIMAL(10,2))) AS INT) >= 0
                    THEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Customer_Service_Calls) AS DECIMAL(10,2))) AS INT)
                ELSE NULL
            END AS customer_service_cells,

             -- Product Reviews: Non-negative integer
             CASE
                WHEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Product_Reviews_Written) AS DECIMAL(10,2))) AS INT) >= 0
                    THEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Product_Reviews_Written) AS DECIMAL(10,2))) AS INT)
                ELSE NULL
             END AS product_reviews_written,

              -- Social Media Score: Non-negative
              CASE
                WHEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Social_Media_Engagement_Score) AS DECIMAL(10,2))) AS INT) >= 0
                    THEN TRY_CAST(FLOOR(TRY_CAST(TRIM(Social_Media_Engagement_Score) AS DECIMAL(10,2))) AS INT)
                ELSE NULL
              END AS social_media_engegement_score,

        -- Mobile App Usage: Non-negative
            CASE 
                WHEN TRY_CAST(Mobile_App_Usage AS DECIMAL(10,2)) >= 0
                    THEN TRY_CAST(ROUND(TRY_CAST(Mobile_App_Usage AS DECIMAL(10,2)), 0) AS INT)
                ELSE NULL
            END AS mobile_app_usage,

            -- Payment Method Diversity: Positive integer (1-10)
            CASE 
                WHEN TRY_CAST(Payment_Method_Diversity AS DECIMAL(10,2)) BETWEEN 1 AND 10
                    THEN TRY_CAST(ROUND(TRY_CAST(Payment_Method_Diversity AS DECIMAL(10,2)), 0) AS INT)
                ELSE NULL
            END AS payment_method_diversity,

            -- Lifetime Value: Non-negative
            CASE 
                WHEN TRY_CAST(Lifetime_Value AS DECIMAL(12,2)) >= 0
                    THEN TRY_CAST(Lifetime_Value AS DECIMAL(12,2))
                ELSE NULL
            END AS lifetime_value,

            -- Credit Balance: Non-negative
            CASE 
                WHEN TRY_CAST(Credit_Balance AS DECIMAL(10,2)) >= 0
                    THEN TRY_CAST(Credit_Balance AS DECIMAL(10,2))
                ELSE NULL
            END AS credit_balance,

             -- Churned: Boolean flag
            CASE 
                WHEN TRY_CAST(Churned AS DECIMAL(3,1)) = 1 THEN 1
                WHEN TRY_CAST(Churned AS DECIMAL(3,1)) = 0 THEN 0
                ELSE NULL
            END AS churned,

            -- Quarter Number
            CASE 
                WHEN Signup_Quarter IN ('Q1','Q2','Q3','Q4')
                    THEN TRY_CAST(SUBSTRING(Signup_Quarter, 2, 1) AS INT)
                ELSE NULL
            END AS signup_quarter_num,

                CASE 
                WHEN Signup_Quarter IN ('Q1','Q2','Q3','Q4')
                    THEN Signup_Quarter
                ELSE NULL
            END AS signup_quarter_label

        FROM bronze.csv_customer_info;

		SET @end_time = GETDATE();

		PRINT '--------------------------------------------';
		PRINT 'Loading Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';

        PRINT 'Entire Sillver Layer is loaded in ';

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

EXEC silver.proc_load_silver;