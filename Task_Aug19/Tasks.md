Scenario: Retail Order Processing Pipeline
 
You are working as a Data Engineer for an e-commerce company. The company receives customer order files continuously from multiple source systems.
 
Order files are delivered to cloud storage in CSV format. The business wants a scalable data pipeline that:
 
1. Ingests newly arriving order files automatically.
2. Stores the raw data in a Delta table.
3. Cleans and transforms the order data using PySpark.
4. Uses RDD operations for selected analytical processing.
5. Uses Spark SQL for business analysis.
6. Maintains the Delta table when orders are inserted, corrected, or cancelled.
7. Supports historical/version-based analysis of the data.
 
Source Data
 
The incoming files contain the following columns:
 
| Column       | Data Type | Description                     |
| ------------ | --------- | ------------------------------- |
| order_id     | String    | Unique order identifier         |
| customer_id  | String    | Customer identifier             |
| product_id   | String    | Product identifier              |
| category     | String    | Product category                |
| quantity     | Integer   | Number of units                 |
| unit_price   | Double    | Price per unit                  |
| order_date   | Date      | Order date                      |
| city         | String    | Customer city                   |
| order_status | String    | COMPLETED, PENDING or CANCELLED |
 
Example:
 
text
order_id,customer_id,product_id,category,quantity,unit_price,order_date,city,order_status
O1001,C101,P501,Electronics,2,25000,2026-08-01,Ahmedabad,COMPLETED
O1002,C102,P502,Fashion,3,1500,2026-08-01,Mumbai,COMPLETED
O1003,C101,P503,Electronics,1,50000,2026-08-02,Ahmedabad,PENDING
O1004,C103,P504,Grocery,5,500,2026-08-02,Pune,COMPLETED
O1005,C104,P505,Fashion,2,2000,2026-08-03,Delhi,CANCELLED
 
 
create 2 .csv files
 
---
 
Part A — PySpark RDD
 
Task 1 — Create and Inspect an RDD
 
Read the order data and convert the required data into an RDD.
 
Perform the following:
 
Display the first 5 records.
Find the total number of records.
Display the number of partitions.
Explain the difference between a Spark DataFrame and an RDD in the context of this scenario.
 
---
 
Task 2 — RDD Transformations
 
 
Using RDD operations only:
 
1. Filter orders having order_status = "COMPLETED".
2. Extract category, quantity, and unit_price.
3. Calculate order_amount as:
 
order_amount = quantity  unit_price
 
 
Return an RDD containing:
 
(category, order_amount)
 
 
Expected structure:
 
("Electronics", 50000)
("Fashion", 4500)
 
 
---
 
Task 3 — Aggregation Using RDD
 
Using the RDD created above, calculate the total completed sales amount for each category.
 
Expected result format:
 
text
Electronics -> 50000
Fashion     -> 4500
Grocery     -> 2500
 
 
---
 
Part B — PySpark DataFrame & Spark SQL
 
Task 4 — Data Cleaning and Transformation
 
Using the DataFrame API:
 
1. Define an explicit schema for the source data.
2. Read the CSV data using the schema.
3. Remove records where order_id or customer_id is null.
4. Remove duplicate records based on order_id.
5. Create a new column:
 
order_amount = quantity  unit_price
 
 
6. Create another column named order_year from order_date.
7. Display the resulting schema and data.
 
---
 
Task 5 — Spark SQL
 
 
Register the cleaned DataFrame as a temporary view named:
 
orders_view
 
 
Write Spark SQL queries to answer the following business questions:
 
1. What is the total sales amount by category?
2. What are the top 3 customers by completed sales amount?
3. Which city has generated the highest completed sales?
4. How many orders exist for each order_status?
5. Find all customers whose completed sales amount is greater than 50,000.
 
The solution should use:
 
---
 
Part C — Delta Table & Delta Operations
 
Task 6 — Create a Delta Table
 
 
Store the cleaned order DataFrame as a Delta table named:
 
retail_orders
 
 
---
 
Task 7 — INSERT
 
 
Insert a new order into retail_orders.
 
Example:
 
O2001,C201,P601,Electronics,1,75000,2026-08-10,Surat,COMPLETED
 
 
Ensure all derived columns required by your Delta table are populated correctly.
 
Verify the inserted record.
 
---
 
Task 8 — UPDATE
 
 
An order was initially marked as PENDING but has now been completed.
 
Update:
 
order_id = O1003
 
 
Change:
 
PENDING -> COMPLETED
 
 
Verify the update.
 
---
 
Task 9 — DELETE
 
 
The business wants cancelled order O1005 removed from the curated table.
 
Delete the record from the Delta table and demonstrate that it no longer exists.
 
---
 
Task 10 — MERGE / UPSERT
 
 
A new batch called order_updates contains both new and existing orders.
 
Example:
 
order_id | customer_id | product_id | quantity | unit_price | order_status
O1002    | C102        | P502       | 4        | 1500       | COMPLETED
O2002    | C202        | P602       | 2        | 3000       | COMPLETED
 
 
Implement a Delta MERGE operation where:
 
If order_id already exists, update the existing record.
If order_id does not exist, insert the new record.
Ensure order_amount remains consistent with quantity  unit_price.
 
---
 
Task 11 — Delta Time Travel
 
Demonstrate Delta Lake time travel.
 
1. Display the history of retail_orders.
2. Identify at least one previous table version.
3. Query that version of the table.
4. Compare the previous version with the current version.
 
---
 
Part D — Databricks Auto Loader
 
Task 12 — Auto Loader Configuration
 
a. Upload the file into volume:
 
retail_orders_1.csv
 
b. Using Databricks Auto Loader, create a streaming Delta table named:
 
retailOrders_streaming
 
Ensure that the streaming pipeline processes newly arriving files incrementally.
 
Task 13 — Course of Business: Category-wise Sales
 
c. Write an SQL query on retailOrders_streaming to retrieve the total completed sales amount for each product category.
 
Calculate:
 
order_amount = quantity * unit_price
 
Consider only records where:
 
order_status = 'COMPLETED'
 
Expected output format:
 
category | total_sales_amount
 
For example:
 
Electronics | 125000
Fashion     | 48500
Grocery     | 32000
 
Task 14 — Incremental File Processing
 
d. Upload the second file:
 
retail_orders_2.csv
 
into the same directory:
 
e. Allow the existing Auto Loader pipeline to ingest the newly arrived file.
 
Do not recreate the streaming table or remove the checkpoint location.
 
Re-run the category-wise sales query from the previous task and observe the updated results.
 
 
Task 15 — Monthly Revenue
 
f. Write an SQL query to calculate the monthly completed sales revenue, considering only data ingested through Auto Loader into:
 
retailOrders_streaming
 
Use the order_date column to derive the revenue month.
 
Consider only:
 
order_status = 'COMPLETED'
 
Expected output format:
 
revenue_month | total_revenue
 
For example:
 
2026-06 | 275000
2026-07 | 315500
2026-08 | 428000
 
Sort the output chronologically by month.
 
The query must read from the Auto Loader-created Delta table and should not directly read the source CSV files.