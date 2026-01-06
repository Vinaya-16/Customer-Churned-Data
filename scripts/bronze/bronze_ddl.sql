
-- CREATING BRONZE TABLE TO STORE RAW DATA

IF OBJECT_ID('bronze.csv_customer_info','U') IS NOT NULL
	DROP TABLE bronze.csv_customer_info;

GO

CREATE TABLE bronze.csv_customer_info
(
	age							  NVARCHAR(50),
	gender						  NVARCHAR(50),
	country						  NVARCHAR(50),
	city						  NVARCHAR(50),
	membership_years			  NVARCHAR(50),
	login_frequency				  NVARCHAR(50),
	Session_Duration_Avg		  NVARCHAR(50),
	pages_per_session			  NVARCHAR(50),
	Cart_Abandonment_Rate		  NVARCHAR(50),
	wishlist_items				  NVARCHAR(50),
	total_purchases				  NVARCHAR(50),
	avg_order_value			      NVARCHAR(50),
	Days_Since_Last_Purchase      NVARCHAR(50),
	Discount_Usage_Rate		      NVARCHAR(50),
	returns_rate			      NVARCHAR(50),
	Email_Open_Rate			      NVARCHAR(50),
	Customer_Service_Calls		  NVARCHAR(50),
	Product_Reviews_Written		  NVARCHAR(50),
	Social_Media_Engagement_Score NVARCHAR(50),
	Mobile_App_Usage			  NVARCHAR(50),
	Payment_Method_Diversity	  NVARCHAR(50),
	Lifetime_Value				  NVARCHAR(50),
	Credit_Balance				  NVARCHAR(50),
	Churned						  NVARCHAR(50),
	Signup_Quarter				  NVARCHAR(50)
);
