
-- Create SQL DDL scripts for all csv files in the crm and erp systems.

IF OBJECT_ID('silver.cust_churn_info', 'U') IS NOT NULL
	DROP TABLE silver.cust_churn_info;

GO

CREATE TABLE silver.cust_churn_info
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
	signup_quarter					NVARCHAR(10),
    dwh_create_date		            DATETIME2 DEFAULT GETDATE()
);
