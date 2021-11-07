-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # SQL Testing On DATABRICKS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC ## Converting date formate of payment csv
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC 
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/Payment_transactions.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "true"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC df = (df
-- MAGIC     .withColumn("Cash_Received_Date", to_date(unix_timestamp("Cash_Received_Date", "d/M/yyyy").cast("timestamp"))))
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Create a view or table
-- MAGIC 
-- MAGIC temp_table_name = "Payment_transactions_csv"
-- MAGIC 
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

SELECT * 
FROM Payment_transactions_csv
LIMIT 5

-- COMMAND ----------

SELECT * 
FROM Employer_master_csv
LIMIT 5

-- COMMAND ----------

SELECT 
    Tier, 
    Month_end_date, 
    COUNT(distinct(Employer_No)) as Num_payments,
    ROUND(SUM(Total_Amt_paid),2) AS Amount_of_payments,
    SUM(New_employers) AS New_employers,
    SUM(Open_employers_at_EOM) AS Open_employers_at_EOM
FROM (
    SELECT 
        -- All the default values 
        PYM.Employer_No,
        PYM.Month_end_date,
        Tier,
        Total_Amt_paid,
        Status,

        -- Using case to convert date's month and year to be in same month to 1 else 0 so later on it will be easier to use SUM
        
        CASE 
            WHEN MONTH(EMP.Effective_From) = MONTH(PYM.Month_end_date) AND YEAR(EMP.Effective_From) = YEAR(PYM.Month_end_date) THEN 1
            ELSE 0
            END
            AS New_employers,

        -- Using case to convert open to 1 else 0 so later on it will be easier to use SUM
        CASE 
            WHEN EMP.Status = 'Open' Then 1
            ELSE 0
            END
            AS Open_employers_at_EOM

    FROM (

        -- getting EMPLOYER NO, END OF MONTH, TOTAL AMOUT PAID using group by which will serve as first table
        SELECT 
            Employer_No, 
            Month_end_date, 
            SUM(Total_Amt) AS Total_Amt_paid
        FROM ( 

            -- getting END of MONTH date from Cash_Received_Date
            SELECT 
                Employer_No, 
                last_day(Cash_Received_Date) AS Month_end_date, 
                Total_Amt
            FROM 
                Payment_transactions_csv
            ) AS 
                Payment_table
        
        -- Here I am using where clause to restrict all date values to be in range of year 2018
        WHERE 
            Month_end_date >= '2018-01-01'
        AND 
            Month_end_date <= '2018-12-31'
        
        -- Aggrigating employer no and month end date
        GROUP BY 
            Employer_No, Month_end_date
    ) AS PYM
    LEFT JOIN ( -- Joining Employer table to payment table
        SELECT 
            *
        FROM 
            Employer_master_csv
    ) AS 
        EMP
    ON 
        PYM.Employer_No=EMP.Employer_No -- Joining condition
    AND 
        (PYM.Month_end_date >= EMP.Effective_From) -- making sure payment month end date is greater than effective_from date of employer
    AND 
        (PYM.Month_end_date <= EMP.Effective_To) -- making sure payment month end date is less than effective_to date of employer
) AS 
    MAIN_TABLE -- Naming Final Table as main table for future use
GROUP BY  
    Tier, Month_end_date
ORDER BY 
    Tier, Month_end_date

-- COMMAND ----------


