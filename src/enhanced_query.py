import duckdb

# Connect to the database file
with duckdb.connect('sales_timeseries.db',read_only=True) as con:

    # Query the data
    print("=== Database Schema ===")
    result = con.execute("DESCRIBE sales_data").fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}")

    print("\n=== Sample Data with Day and Month Names ===")
    df = con.execute("""
        SELECT date, total_amount_per_product_sgd, day_of_week_text, month, month_text
        FROM sales_data 
        LIMIT 10
    """).df()
    print(df)

    print("\n=== Sales by Day of Week (Text) ===")
    dow_stats = con.execute("""
        SELECT 
            day_of_week_text,
            AVG(total_amount_per_product_sgd) as avg_sales,
            SUM(total_amount_per_product_sgd) as total_sales,
            COUNT(*) as count
        FROM sales_data 
        GROUP BY day_of_week_text, day_of_week
        ORDER BY day_of_week
    """).df()
    print(dow_stats)

    print("\n=== Sales by Month (Text) ===")
    month_stats = con.execute("""
        SELECT 
            month_text,
            AVG(total_amount_per_product_sgd) as avg_sales,
            SUM(total_amount_per_product_sgd) as total_sales,
            COUNT(*) as count
        FROM sales_data 
        GROUP BY month_text, month
        ORDER BY month
    """).df()
    print(month_stats)

    print("\n=== Top 5 Sales Days with Day and Month Names ===")
    top_days = con.execute("""
        SELECT date, total_amount_per_product_sgd, day_of_week_text, month_text
        FROM sales_data 
        ORDER BY total_amount_per_product_sgd DESC
        LIMIT 5
    """).df()
    print(top_days)

    # Close the connection

