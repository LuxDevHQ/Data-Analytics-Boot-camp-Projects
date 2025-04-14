
# Connect PySpark to PostgreSQL and Run Debt Analysis Queries

## **🧩 Step 1: Install Dependencies**
Make sure you have PySpark installed:

```bash
pip install pyspark
```

Download the PostgreSQL JDBC driver:
➡️ PostgreSQL JDBC Download

Assume you saved it here:

```bash
/path/to/postgresql-42.2.5.jar
```

## **🧩 Step 2: Connect to PostgreSQL Database**
```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PostgreSQL with PySpark") \
    .config("spark.jars", "/path/to/postgresql-42.2.5.jar") \
    .getOrCreate()

# Read data from PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.178.131.221:5432/warehouse") \
    .option("dbtable", "dataanalytics.international_debt") \
    .option("user", "luxds") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Check schema and preview data
df.printSchema()
df.show(5)
```

## **🧩 Step 3: Register DataFrame as Temporary SQL View**
```python
df.createOrReplaceTempView("international_debt")
```

## **📝 Step 4: Analytical Queries**

### **1️⃣ Total Amount of Debt Owed by All Countries**
```python
spark.sql("""
    SELECT SUM(debt) AS total_debt
    FROM international_debt
""").show()
```

### **2️⃣ Number of Distinct Countries**
```python
spark.sql("""
    SELECT COUNT(DISTINCT country_name) AS distinct_countries
    FROM international_debt
""").show()
```

### **3️⃣ Distinct Types of Debt Indicators and Their Meaning**
```python
spark.sql("""
    SELECT DISTINCT indicator_code, indicator_name
    FROM international_debt
""").show(truncate=False)
```

### **4️⃣ Country with the Highest Total Debt**
```python
spark.sql("""
    SELECT country_name, SUM(debt) AS total_debt
    FROM international_debt
    GROUP BY country_name
    ORDER BY total_debt DESC
    LIMIT 1
""").show()
```

### **5️⃣ Average Debt Across Different Debt Indicators**
```python
spark.sql("""
    SELECT indicator_name, AVG(debt) AS average_debt
    FROM international_debt
    GROUP BY indicator_name
""").show(truncate=False)
```

### **6️⃣ Country with the Highest Principal Repayments**
Assuming indicator_name contains "Principal repayments".

```python
spark.sql("""
    SELECT country_name, SUM(debt) AS total_principal_repayment
    FROM international_debt
    WHERE indicator_name ILIKE '%principal repayment%'
    GROUP BY country_name
    ORDER BY total_principal_repayment DESC
    LIMIT 1
""").show()
```

### **7️⃣ Most Common Debt Indicator**
```python
spark.sql("""
    SELECT indicator_name, COUNT(*) AS frequency
    FROM international_debt
    GROUP BY indicator_name
    ORDER BY frequency DESC
    LIMIT 1
""").show()
```

### **8️⃣ Trends and Exploratory Insights**

#### **📊 Trend by Year**
```python
spark.sql("""
    SELECT year, SUM(debt) AS total_debt
    FROM international_debt
    GROUP BY year
    ORDER BY year
""").show()
```

#### **📊 Top 5 Countries by Total Debt**
```python
spark.sql("""
    SELECT country_name, SUM(debt) AS total_debt
    FROM international_debt
    GROUP BY country_name
    ORDER BY total_debt DESC
    LIMIT 5
""").show()
```

#### **📊 Debt Indicators Over Time**
```python
spark.sql("""
    SELECT year, indicator_name, SUM(debt) AS total_debt
    FROM international_debt
    GROUP BY year, indicator_name
    ORDER BY year, total_debt DESC
""").show(truncate=False)
```

## **✅ Step 5: (Optional) Export Results to CSV**
You can export any result to CSV for reporting:

```python
result = spark.sql("SELECT country_name, SUM(debt) AS total_debt FROM international_debt GROUP BY country_name")
result.write.csv("/path/to/output.csv", header=True)
```

## **🎉 Summary**
With this PySpark + PostgreSQL integration, you are now able to:

- Connect Spark to external PostgreSQL database
- Run advanced SQL analytics on international debt data
- Export your insights for reporting or visualization

## **🚀 Optional Enhancements**
- ✅ Create .py script for automation
- ✅ Use Jupyter Notebook for interactive exploration
- ✅ Visualize with matplotlib or seaborn
- ✅ Schedule jobs using Airflow or Cron for daily reports
