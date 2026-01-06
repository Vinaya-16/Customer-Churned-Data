-- CREATING SILVER SCHEMA TO STORE CLEAN DATA

IF OBJECT_ID('silver.csv_customer_info','U') IS NOT NULL
	DROP TABLE silver.csv_customer_info;

GO

CREATE TABLE silver.csv_customer_info
(
	age							  INT,

	gender						  NVARCHAR(50),
	country						  NVARCHAR(50),
	city						  NVARCHAR(50),

	membership_years			  DECIMAL(5,2),

	login_frequency				  INT,
	Session_Duration_Avg		  DECIMAL(6,2),
	pages_per_session			  DECIMAL(6,2),

	Cart_Abandonment_Rate		  DECIMAL(5,2),

	wishlist_items				  INT,
	total_purchases				  INT,
	avg_order_value			      DECIMAL(10,2),

	Days_Since_Last_Purchase      INT,

	Discount_Usage_Rate		      DECIMAL(5,2),
	returns_rate			      DECIMAL(5,2),
	Email_Open_Rate			      DECIMAL(5,2),

	Customer_Service_Calls		  INT,
	Product_Reviews_Written		  INT,
	Social_Media_Engagement_Score INT,
	Mobile_App_Usage			  INT,

	Payment_Method_Diversity	  INT,

	Lifetime_Value				  DECIMAL(12,2),
	Credit_Balance				  DECIMAL(10,2),

	Churned						  BIT,

	signup_quarter_num			  INT,
	signup_quarter_label		  CHAR(2),

	dwh_load_date				  DATETIME DEFAULT GETDATE()
);

