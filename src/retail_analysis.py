import duckdb

# Connect to the database file using context manager
with duckdb.connect('sales_timeseries.db', read_only=True) as con:
    print("=== Retail Sales Database Schema ===")
    result = con.execute("DESCRIBE sales_data").fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}")

    print("\n=== Database Summary ===")
    summary = con.execute("""
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(DISTINCT customer_number) as unique_customers,
            COUNT(DISTINCT receipt_number) as unique_receipts,
            COUNT(DISTINCT product_id) as unique_products,
            MIN(date) as start_date,
            MAX(date) as end_date,
            SUM(total_amount_per_product_sgd) as total_revenue_sgd,
            AVG(total_amount_per_product_sgd) as avg_amount_per_item,
            AVG(receipt_total_sgd) as avg_receipt_total
        FROM sales_data
    """).fetchone()

    print(f"Total transactions: {summary[0]:,}")
    print(f"Unique customers: {summary[1]:,}")
    print(f"Unique receipts: {summary[2]:,}")
    print(f"Unique products: {summary[3]:,}")
    print(f"Date range: {summary[4]} to {summary[5]}")
    print(f"Total revenue: SGD ${summary[6]:,.2f}")
    print(f"Average amount per item: SGD ${summary[7]:.2f}")
    print(f"Average receipt total: SGD ${summary[8]:.2f}")

    print("\n=== Sample Transaction Data ===")
    sample = con.execute("""
        SELECT 
            date,
            customer_number,
            receipt_number,
            product_name,
            units_sold,
            unit_price_sgd,
            total_amount_per_product_sgd,
            receipt_total_sgd,
            day_of_week_text,
            month_text
        FROM sales_data 
        ORDER BY date
        LIMIT 10
    """).df()
    print(sample)

    print("\n=== Top 5 Products by Revenue ===")
    top_products = con.execute("""
        SELECT 
            product_name,
            SUM(total_amount_per_product_sgd) as total_revenue,
            SUM(units_sold) as total_units_sold,
            COUNT(*) as transactions,
            AVG(unit_price_sgd) as avg_price
        FROM sales_data 
        GROUP BY product_id, product_name
        ORDER BY total_revenue DESC
        LIMIT 5
    """).df()
    print(top_products)

    print("\n=== Sales by Day of Week ===")
    dow_sales = con.execute("""
        SELECT 
            day_of_week_text,
            SUM(total_amount_per_product_sgd) as total_revenue,
            COUNT(DISTINCT receipt_number) as num_receipts,
            AVG(receipt_total_sgd) as avg_receipt_value
        FROM sales_data 
        GROUP BY day_of_week_text, day_of_week
        ORDER BY day_of_week
    """).df()
    print(dow_sales)

    print("\n=== Sales by Month ===")
    month_sales = con.execute("""
        SELECT 
            month_text,
            SUM(total_amount_per_product_sgd) as total_revenue,
            COUNT(DISTINCT receipt_number) as num_receipts,
            COUNT(DISTINCT customer_number) as unique_customers
        FROM sales_data 
        GROUP BY month_text, month
        ORDER BY month
    """).df()
    print(month_sales)

    print("\n=== Hourly Sales Pattern ===")
    hourly_sales = con.execute("""
        SELECT 
            hour,
            SUM(total_amount_per_product_sgd) as total_revenue,
            COUNT(*) as transactions,
            AVG(receipt_total_sgd) as avg_receipt_value
        FROM sales_data 
        GROUP BY hour
        ORDER BY hour
    """).df()
    print(hourly_sales)

    print("\n=== Sales by Age Groups ===")
    age_group_sales = con.execute("""
        SELECT 
            CASE 
                WHEN age < 25 THEN '1. Under 25'
                WHEN age BETWEEN 25 AND 40 THEN '2. 25 to 40'
                WHEN age >= 41 THEN '3. 41 and above'
                ELSE 'Unknown'
            END as age_group,
            COUNT(*) as total_transactions,
            SUM(total_amount_per_product_sgd) as total_revenue,
            COUNT(DISTINCT customer_number) as unique_customers,
            COUNT(DISTINCT receipt_number) as unique_receipts,
            AVG(total_amount_per_product_sgd) as avg_transaction_value,
            AVG(receipt_total_sgd) as avg_receipt_value,
            MIN(age) as min_age,
            MAX(age) as max_age,
            AVG(age) as avg_age
        FROM sales_data 
        WHERE age IS NOT NULL
        GROUP BY 
            CASE 
                WHEN age < 25 THEN '1. Under 25'
                WHEN age BETWEEN 25 AND 40 THEN '2. 25 to 40'
                WHEN age >= 41 THEN '3. 41 and above'
                ELSE 'Unknown'
            END
        ORDER BY age_group
    """).df()
    print(age_group_sales)

    print("\n=== Age Group Revenue Distribution ===")
    total_revenue = age_group_sales['total_revenue'].sum()
    for _, row in age_group_sales.iterrows():
        percentage = (row['total_revenue'] / total_revenue) * 100
        print(f"{row['age_group']}: SGD ${row['total_revenue']:,.2f} ({percentage:.1f}% of total)")

    print("\n=== Top Products by Age Group ===")
    for age_group in ['1. Under 25', '2. 25 to 40', '3. 41 and above']:
        print(f"\n--- {age_group} ---")
        top_products_by_age = con.execute(f"""
            SELECT 
                product_name,
                SUM(total_amount_per_product_sgd) as total_revenue,
                SUM(units_sold) as total_units_sold,
                COUNT(*) as transactions,
                AVG(unit_price_sgd) as avg_price
            FROM sales_data 
            WHERE CASE 
                WHEN age < 25 THEN '1. Under 25'
                WHEN age BETWEEN 25 AND 40 THEN '2. 25 to 40'
                WHEN age >= 41 THEN '3. 41 and above'
                ELSE 'Unknown'
            END = '{age_group}'
            GROUP BY product_id, product_name
            ORDER BY total_revenue DESC
            LIMIT 3
        """).df()
        
        print(top_products_by_age)
