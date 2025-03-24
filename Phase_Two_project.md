**These questions for the second day of week 10 in the Data Analytics Bootcamp are designed to challenge you to improve your SQL querying, aggregation, window functions, CTEs, and analytical skills.**
## 🟢 Intermediate-Level Questions (1-10)

1. **Find the total number of transactions per city.**  
   - *Hint: Use `JOIN` with `GROUP BY`.*

2. **Retrieve the top 5 most purchased products based on total quantity sold.**  
   - *Hint: Use `GROUP BY` and `ORDER BY`.*

3. **Find the average transaction amount per category.**  
   - *Hint: Use `AVG()` with `GROUP BY`.*

4. **Identify the payment method that has the highest total sales.**  
   - *Hint: Use `SUM(total_amount)` with `GROUP BY payment_method`.*

5. **Find customers who have made at least 5 transactions.**  
   - *Hint: Use `COUNT(transaction_id) HAVING COUNT(transaction_id) >= 5`.*

6. **Retrieve all customers who registered in the last 6 months but have not made any transactions.**  
   - *Hint: Use `LEFT JOIN` and `IS NULL`.*

7. **Find the total revenue generated in each year from sales transactions.**  
   - *Hint: Use `DATE_PART('year', transaction_date)` or `EXTRACT(YEAR FROM transaction_date)`.*

8. **List the number of unique products sold in each category.**  
   - *Hint: Use `COUNT(DISTINCT product_name)`.*

9. **Find all customers who have made purchases across at least 3 different product categories.**  
   - *Hint: Use `COUNT(DISTINCT category) HAVING >= 3`.*

10. **Identify the most popular purchase day of the week based on transaction count.**  
   - *Hint: Use `TO_CHAR(transaction_date, 'Day')` or `EXTRACT(DOW FROM transaction_date)`.*

---

## 🔴 Advanced-Level Questions (11-20)

11. **Find the top 3 customers who have spent the most in the last 12 months.**  
    - *Hint: Use `SUM(total_amount)` and filter on `transaction_date`.*

12. **Determine the percentage of total revenue contributed by each product category.**  
    - *Hint: Use `SUM(total_amount) * 100.0 / (SELECT SUM(total_amount) FROM sales_transactions)`.*

13. **Find the month-over-month sales growth for the last 12 months.**  
    - *Hint: Use `LAG(SUM(total_amount)) OVER (ORDER BY transaction_date)` to compare months.*

14. **Identify customers who have increased their spending by at least 30% compared to the previous year.**  
    - *Hint: Use `SUM(total_amount) FILTER (WHERE YEAR = X)` vs `SUM(total_amount) FILTER (WHERE YEAR = X-1)`.*

15. **Find the first purchase date for each customer.**
