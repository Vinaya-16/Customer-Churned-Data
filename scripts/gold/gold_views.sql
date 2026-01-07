--==============================================================================
--	CREATE A VIEW TABLES FOR GOLD LAYER
--==============================================================================


-- CREATE A VIEW FOR CUSTOMER INFORMATION 
-- dim_cust_info

IF OBJECT_ID('gold.dim_cust_info', 'V') IS NOT NULL
    DROP VIEW gold.dim_cust_info;

GO

CREATE OR ALTER VIEW gold.dim_cust_info AS
	SELECT
	age,
	gender,
	membership_years
	FROM silver.csv_customer_info;

GO

-- CRAETE A VIEW FOR LOCATION OF CUSTOMERS
-- dim_cust_loc

IF OBJECT_ID('gold.dim_cust_loc', 'V') IS NOT NULL
	DROP VIEW gold.dim_cust_loc;

GO

CREATE OR ALTER VIEW gold.dim_cust_loc AS
SELECT
	country,
	city
FROM silver.csv_customer_info;

GO

-- CREATE A VIEW FOR TIMELINESS OF THE CUSTOMER
-- dim_time

IF OBJECT_ID('gold.dim_time', 'V') IS NOT NULL
	DROP VIEW gold.dim_time;
GO

CREATE OR ALTER VIEW gold.dim_time AS
	SELECT
	signup_quarter_label,
	signup_quarter_num
	FROM silver.csv_customer_info;

GO

-- CREATE A VIEW FOR CUTSOMER'S BEHAVIOUR
-- fact_cust_behaviour

IF OBJECT_ID('gold.fact_cust_behaviour', 'V') IS NOT NULL
	DROP VIEW gold.fact_cust_behaviour;

GO

CREATE OR ALTER VIEW gold.fact_cust_behaviour AS
	SELECT
	login_frequency,
	Session_Duration_Avg,
	pages_per_session,
	Cart_Abandonment_Rate,
	wishlist_items
	FROM silver.csv_customer_info;

GO

-- CREATE A VIEW FOR CUSTOMER'S VALUE
-- fact_cust_value

IF OBJECT_ID('gold.fact_cust_value', 'V') IS NOT NULL
	DROP VIEW gold.fact_cust_value;

GO

CREATE OR ALTER VIEW gold.fact_cust_value AS
	SELECT
	total_purchases,
	avg_order_value,
	Lifetime_Value,
	Credit_Balance
	FROM silver.csv_customer_info;

GO

-- CREATE A VIEW FOR EXTRACTING CUSTOMER'S CHURN VALUES
-- fact_cust_churn

IF OBJECT_ID('gold.fact_cust_churn', 'V') IS NOT NULL
	DROP VIEW gold.fact_cust_churn;

GO

CREATE OR ALTER VIEW gold.fact_cust_churn AS
	SELECT
	churned,
	returns_rate,
	discount_usage_rate,
	customer_service_calls
	FROM silver.csv_customer_info;
