import duckdb
import numpy as np
import matplotlib.pyplot as plt

# Connect to the database file using context manager
with duckdb.connect('sales_timeseries.db', read_only=True) as con:
    print("=== Updated Database Schema ===")
    result = con.execute("DESCRIBE sales_data").fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}")

    print("\n=== Database Summary ===")
    summary = con.execute("""
        SELECT 
            COUNT(*) as total_rows,
            MIN(date) as start_date,
            MAX(date) as end_date,
            AVG(total_amount_per_product_sgd) as avg_sales,
            MIN(total_amount_per_product_sgd) as min_sales,
            MAX(total_amount_per_product_sgd) as max_sales
        FROM sales_data
        WHERE transaction_desc = 'Product Sale'
    """).fetchone()

    print(f"Total sales transactions: {summary[0]}")
    print(f"Date range: {summary[1]} to {summary[2]}")
    print(f"Sales - Average: {summary[3]:.2f}, Min: {summary[4]}, Max: {summary[5]}")

    print("\n=== Best Day and Month for Sales (Sales Only) ===")
    best_day = con.execute("""
        SELECT day_of_week_text, SUM(total_amount_per_product_sgd) as total_sales
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY day_of_week_text, day_of_week
        ORDER BY total_sales DESC 
        LIMIT 1
    """).fetchone()

    best_month = con.execute("""
        SELECT month_text, SUM(total_amount_per_product_sgd) as total_sales
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY month_text, month
        ORDER BY total_sales DESC 
        LIMIT 1
    """).fetchone()

    print(f"Best day: {best_day[0]} with SGD ${best_day[1]:,.2f} total sales")
    print(f"Best month: {best_month[0]} with SGD ${best_month[1]:,.2f} total sales")

    print("=== Daily Sales Performance Analysis ===")
    daily_sales = con.execute("""
        SELECT 
            DATE(date) as sales_date,
            day_of_week_text,
            month_text,
            EXTRACT(year FROM date) as year,
            COUNT(*) as total_transactions,
            SUM(total_amount_per_product_sgd) as daily_revenue,
            AVG(total_amount_per_product_sgd) as avg_transaction_value,
            COUNT(DISTINCT customer_number) as unique_customers,
            COUNT(DISTINCT receipt_number) as unique_receipts
        FROM sales_data 
        GROUP BY DATE(date), day_of_week_text, month_text, EXTRACT(year FROM date)
        ORDER BY daily_revenue DESC
    """).df()
    
    print("Top 10 Highest Revenue Days:")
    print(daily_sales.head(10)[['sales_date', 'day_of_week_text', 'month_text', 'year', 'daily_revenue', 'total_transactions']])
    
    print("Bottom 10 Lowest Revenue Days:")
    print(daily_sales.tail(10)[['sales_date', 'day_of_week_text', 'month_text', 'year', 'daily_revenue', 'total_transactions']])
    
    print("=== 🏆 Daily Sales Highlights ===")
    best_day_row = daily_sales.iloc[0]
    worst_day_row = daily_sales.iloc[-1]
    
    print("RECORD BREAKING DAYS:")
    print(f"🥇 Best Single Day: {best_day_row['sales_date']} ({best_day_row['day_of_week_text']}, {best_day_row['month_text']} {int(best_day_row['year'])})")
    print(f"   💰 Revenue: SGD ${best_day_row['daily_revenue']:,.2f}")
    print(f"   📊 Transactions: {int(best_day_row['total_transactions']):,}")
    print(f"   👥 Customers: {int(best_day_row['unique_customers']):,}")
    print(f"   🎯 Avg Transaction: SGD ${best_day_row['avg_transaction_value']:.2f}")
    
    print(f"🥉 Lowest Single Day: {worst_day_row['sales_date']} ({worst_day_row['day_of_week_text']}, {worst_day_row['month_text']} {int(worst_day_row['year'])})")
    print(f"   💸 Revenue: SGD ${worst_day_row['daily_revenue']:,.2f}")
    print(f"   📉 Transactions: {int(worst_day_row['total_transactions']):,}")
    print(f"   👥 Customers: {int(worst_day_row['unique_customers']):,}")
    print(f"   🎯 Avg Transaction: SGD ${worst_day_row['avg_transaction_value']:.2f}")
    
    # Additional insights
    max_transactions_day = daily_sales.loc[daily_sales['total_transactions'].idxmax()]
    min_transactions_day = daily_sales.loc[daily_sales['total_transactions'].idxmin()]
    max_customers_day = daily_sales.loc[daily_sales['unique_customers'].idxmax()]
    best_avg_transaction_day = daily_sales.loc[daily_sales['avg_transaction_value'].idxmax()]
    
    print("OTHER NOTABLE DAYS:")
    print(f"📈 Most Transactions: {max_transactions_day['sales_date']} ({int(max_transactions_day['total_transactions']):,} transactions)")
    print(f"📉 Fewest Transactions: {min_transactions_day['sales_date']} ({int(min_transactions_day['total_transactions']):,} transactions)")
    print(f"👥 Most Customers: {max_customers_day['sales_date']} ({int(max_customers_day['unique_customers']):,} customers)")
    print(f"💎 Best Avg Value: {best_avg_transaction_day['sales_date']} (SGD ${best_avg_transaction_day['avg_transaction_value']:.2f})")
    
    print("DAILY SALES STATISTICS:")
    print(f"📊 Total Days Analyzed: {len(daily_sales):,}")
    print(f"💰 Average Daily Revenue: SGD ${daily_sales['daily_revenue'].mean():,.2f}")
    print(f"📈 Daily Revenue Range: SGD ${daily_sales['daily_revenue'].min():,.2f} - ${daily_sales['daily_revenue'].max():,.2f}")
    print(f"📊 Average Daily Transactions: {daily_sales['total_transactions'].mean():.0f}")
    print(f"👥 Average Daily Customers: {daily_sales['unique_customers'].mean():.0f}")

    print("\n=== Sales by Country ===")
    country_sales = con.execute("""
        SELECT 
            country,
            COUNT(*) as total_transactions,
            SUM(total_amount_per_product_sgd) as total_revenue,
            AVG(total_amount_per_product_sgd) as avg_transaction_value,
            COUNT(DISTINCT customer_number) as unique_customers
        FROM sales_data 
        GROUP BY country
        ORDER BY total_revenue DESC
    """).df()
    print(country_sales)
    
    print("\n=== 🏆 Country Performance Highlights ===")
    print("HIGHEST PERFORMERS:")
    print(f"📈 Most Transactions: {country_sales.iloc[0]['country']} ({country_sales.iloc[0]['total_transactions']:,} transactions)")
    print(f"💰 Highest Revenue: {country_sales.iloc[0]['country']} (SGD ${country_sales.iloc[0]['total_revenue']:,.2f})")
    print(f"🎯 Best Avg Transaction: {country_sales.loc[country_sales['avg_transaction_value'].idxmax()]['country']} (SGD ${country_sales['avg_transaction_value'].max():.2f})")
    print(f"👥 Most Customers: {country_sales.loc[country_sales['unique_customers'].idxmax()]['country']} ({country_sales['unique_customers'].max():,} customers)")
    
    print("\nLOWEST PERFORMERS:")
    print(f"📉 Fewest Transactions: {country_sales.iloc[-1]['country']} ({country_sales.iloc[-1]['total_transactions']:,} transactions)")
    print(f"💸 Lowest Revenue: {country_sales.iloc[-1]['country']} (SGD ${country_sales.iloc[-1]['total_revenue']:,.2f})")
    print(f"🎯 Lowest Avg Transaction: {country_sales.loc[country_sales['avg_transaction_value'].idxmin()]['country']} (SGD ${country_sales['avg_transaction_value'].min():.2f})")
    print(f"👥 Fewest Customers: {country_sales.loc[country_sales['unique_customers'].idxmin()]['country']} ({country_sales['unique_customers'].min():,} customers)")

    print("\n=== Age Groups by Country ===")
    age_country_analysis = con.execute("""
        SELECT 
            country,
            CASE 
                WHEN age < 25 THEN 'Under 25'
                WHEN age BETWEEN 25 AND 40 THEN '25-40'
                WHEN age >= 41 THEN '41+'
                ELSE 'Unknown'
            END as age_group,
            COUNT(*) as total_transactions,
            SUM(total_amount_per_product_sgd) as total_revenue,
            AVG(total_amount_per_product_sgd) as avg_transaction_value,
            COUNT(DISTINCT customer_number) as unique_customers,
            AVG(age) as avg_age_in_group
        FROM sales_data 
        WHERE age IS NOT NULL
        GROUP BY country, age_group
        ORDER BY country, 
            CASE age_group
                WHEN 'Under 25' THEN 1
                WHEN '25-40' THEN 2
                WHEN '41+' THEN 3
                ELSE 4
            END
    """).df()
    print(age_country_analysis)
    
    print("\n=== 🏆 Age Group Performance Highlights by Country ===")
    # Find best and worst performing age groups
    best_revenue_row = age_country_analysis.loc[age_country_analysis['total_revenue'].idxmax()]
    worst_revenue_row = age_country_analysis.loc[age_country_analysis['total_revenue'].idxmin()]
    best_avg_transaction = age_country_analysis.loc[age_country_analysis['avg_transaction_value'].idxmax()]
    worst_avg_transaction = age_country_analysis.loc[age_country_analysis['avg_transaction_value'].idxmin()]
    
    print("HIGHEST PERFORMERS:")
    print(f"💰 Best Revenue: {best_revenue_row['country']} - {best_revenue_row['age_group']} (SGD ${best_revenue_row['total_revenue']:,.2f})")
    print(f"🎯 Best Avg Transaction: {best_avg_transaction['country']} - {best_avg_transaction['age_group']} (SGD ${best_avg_transaction['avg_transaction_value']:.2f})")
    print(f"👥 Most Customers: {age_country_analysis.loc[age_country_analysis['unique_customers'].idxmax()]['country']} - {age_country_analysis.loc[age_country_analysis['unique_customers'].idxmax()]['age_group']} ({age_country_analysis['unique_customers'].max():,} customers)")
    
    print("\nLOWEST PERFORMERS:")
    print(f"💸 Lowest Revenue: {worst_revenue_row['country']} - {worst_revenue_row['age_group']} (SGD ${worst_revenue_row['total_revenue']:,.2f})")
    print(f"🎯 Lowest Avg Transaction: {worst_avg_transaction['country']} - {worst_avg_transaction['age_group']} (SGD ${worst_avg_transaction['avg_transaction_value']:.2f})")
    print(f"👥 Fewest Customers: {age_country_analysis.loc[age_country_analysis['unique_customers'].idxmin()]['country']} - {age_country_analysis.loc[age_country_analysis['unique_customers'].idxmin()]['age_group']} ({age_country_analysis['unique_customers'].min():,} customers)")

    print("\n=== Age Distribution Summary by Country ===")
    age_distribution = con.execute("""
        SELECT 
            country,
            COUNT(CASE WHEN age < 25 THEN 1 END) as under_25_customers,
            COUNT(CASE WHEN age BETWEEN 25 AND 40 THEN 1 END) as age_25_40_customers,
            COUNT(CASE WHEN age >= 41 THEN 1 END) as age_41_plus_customers,
            ROUND(AVG(CASE WHEN age < 25 THEN age END), 1) as avg_age_under_25,
            ROUND(AVG(CASE WHEN age BETWEEN 25 AND 40 THEN age END), 1) as avg_age_25_40,
            ROUND(AVG(CASE WHEN age >= 41 THEN age END), 1) as avg_age_41_plus,
            ROUND(AVG(age), 1) as overall_avg_age
        FROM sales_data 
        WHERE age IS NOT NULL
        GROUP BY country
        ORDER BY overall_avg_age DESC
    """).df()
    print(age_distribution)
    
    print("\n=== 🏆 Age Distribution Highlights ===")
    oldest_country = age_distribution.loc[age_distribution['overall_avg_age'].idxmax()]
    youngest_country = age_distribution.loc[age_distribution['overall_avg_age'].idxmin()]
    most_young_customers = age_distribution.loc[age_distribution['under_25_customers'].idxmax()]
    most_senior_customers = age_distribution.loc[age_distribution['age_41_plus_customers'].idxmax()]
    
    print("DEMOGRAPHIC LEADERS:")
    print(f"👴 Oldest Market: {oldest_country['country']} (avg age: {oldest_country['overall_avg_age']} years)")
    print(f"👶 Youngest Market: {youngest_country['country']} (avg age: {youngest_country['overall_avg_age']} years)")
    print(f"🧒 Most Young Customers: {most_young_customers['country']} ({most_young_customers['under_25_customers']:,} under-25 customers)")
    print(f"👴 Most Senior Customers: {most_senior_customers['country']} ({most_senior_customers['age_41_plus_customers']:,} 41+ customers)")
    
    print("\nAGE EXTREMES:")
    print(f"🔝 Highest Avg Age (Under 25): {age_distribution.loc[age_distribution['avg_age_under_25'].idxmax()]['country']} ({age_distribution['avg_age_under_25'].max():.1f} years)")
    print(f"🔝 Highest Avg Age (25-40): {age_distribution.loc[age_distribution['avg_age_25_40'].idxmax()]['country']} ({age_distribution['avg_age_25_40'].max():.1f} years)")
    print(f"🔝 Highest Avg Age (41+): {age_distribution.loc[age_distribution['avg_age_41_plus'].idxmax()]['country']} ({age_distribution['avg_age_41_plus'].max():.1f} years)")
    print(f"🔻 Lowest Avg Age (Under 25): {age_distribution.loc[age_distribution['avg_age_under_25'].idxmin()]['country']} ({age_distribution['avg_age_under_25'].min():.1f} years)")
    print(f"🔻 Lowest Avg Age (25-40): {age_distribution.loc[age_distribution['avg_age_25_40'].idxmin()]['country']} ({age_distribution['avg_age_25_40'].min():.1f} years)")
    print(f"🔻 Lowest Avg Age (41+): {age_distribution.loc[age_distribution['avg_age_41_plus'].idxmin()]['country']} ({age_distribution['avg_age_41_plus'].min():.1f} years)")

    print("\n=== Revenue Percentage by Age Group per Country ===")
    revenue_percentage = con.execute("""
        WITH country_totals AS (
            SELECT 
                country,
                SUM(total_amount_per_product_sgd) as country_total_revenue
            FROM sales_data 
            WHERE age IS NOT NULL
            GROUP BY country
        ),
        age_group_revenue AS (
            SELECT 
                country,
                CASE 
                    WHEN age < 25 THEN 'Under 25'
                    WHEN age BETWEEN 25 AND 40 THEN '25-40'
                    WHEN age >= 41 THEN '41+'
                    ELSE 'Unknown'
                END as age_group,
                SUM(total_amount_per_product_sgd) as age_group_revenue
            FROM sales_data 
            WHERE age IS NOT NULL
            GROUP BY country, age_group
        )
        SELECT 
            agr.country,
            agr.age_group,
            agr.age_group_revenue,
            ct.country_total_revenue,
            ROUND((agr.age_group_revenue / ct.country_total_revenue) * 100, 2) as revenue_percentage
        FROM age_group_revenue agr
        JOIN country_totals ct ON agr.country = ct.country
        ORDER BY agr.country, 
            CASE agr.age_group
                WHEN 'Under 25' THEN 1
                WHEN '25-40' THEN 2
                WHEN '41+' THEN 3
                ELSE 4
            END
    """).df()
    print(revenue_percentage)
    
    print("\n=== 🏆 Revenue Percentage Highlights ===")
    # Group by age group to find patterns
    under_25_max = revenue_percentage[revenue_percentage['age_group'] == 'Under 25'].loc[revenue_percentage[revenue_percentage['age_group'] == 'Under 25']['revenue_percentage'].idxmax()]
    under_25_min = revenue_percentage[revenue_percentage['age_group'] == 'Under 25'].loc[revenue_percentage[revenue_percentage['age_group'] == 'Under 25']['revenue_percentage'].idxmin()]
    
    age_25_40_max = revenue_percentage[revenue_percentage['age_group'] == '25-40'].loc[revenue_percentage[revenue_percentage['age_group'] == '25-40']['revenue_percentage'].idxmax()]
    age_25_40_min = revenue_percentage[revenue_percentage['age_group'] == '25-40'].loc[revenue_percentage[revenue_percentage['age_group'] == '25-40']['revenue_percentage'].idxmin()]
    
    age_41_plus_max = revenue_percentage[revenue_percentage['age_group'] == '41+'].loc[revenue_percentage[revenue_percentage['age_group'] == '41+']['revenue_percentage'].idxmax()]
    age_41_plus_min = revenue_percentage[revenue_percentage['age_group'] == '41+'].loc[revenue_percentage[revenue_percentage['age_group'] == '41+']['revenue_percentage'].idxmin()]
    
    print("HIGHEST REVENUE CONTRIBUTION BY AGE GROUP:")
    print(f"🧒 Under 25: {under_25_max['country']} ({under_25_max['revenue_percentage']:.2f}%)")
    print(f"👨 25-40: {age_25_40_max['country']} ({age_25_40_max['revenue_percentage']:.2f}%)")
    print(f"👴 41+: {age_41_plus_max['country']} ({age_41_plus_max['revenue_percentage']:.2f}%)")
    
    print("\nLOWEST REVENUE CONTRIBUTION BY AGE GROUP:")
    print(f"🧒 Under 25: {under_25_min['country']} ({under_25_min['revenue_percentage']:.2f}%)")
    print(f"👨 25-40: {age_25_40_min['country']} ({age_25_40_min['revenue_percentage']:.2f}%)")
    print(f"👴 41+: {age_41_plus_min['country']} ({age_41_plus_min['revenue_percentage']:.2f}%)")
    
    print("\nOVERALL REVENUE EXTREMES:")
    overall_max = revenue_percentage.loc[revenue_percentage['revenue_percentage'].idxmax()]
    overall_min = revenue_percentage.loc[revenue_percentage['revenue_percentage'].idxmin()]
    print(f"🔝 Highest Single Contribution: {overall_max['country']} - {overall_max['age_group']} ({overall_max['revenue_percentage']:.2f}%)")
    print(f"🔻 Lowest Single Contribution: {overall_min['country']} - {overall_min['age_group']} ({overall_min['revenue_percentage']:.2f}%)")

    print("\n=== 🕐 Best and Worst Times of Day Analysis ===")
    hourly_sales = con.execute("""
        SELECT 
            hour,
            COUNT(*) as total_transactions,
            SUM(total_amount_per_product_sgd) as total_revenue,
            AVG(total_amount_per_product_sgd) as avg_transaction_value,
            COUNT(DISTINCT customer_number) as unique_customers,
            COUNT(DISTINCT receipt_number) as unique_receipts
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY hour
        ORDER BY hour
    """).df()
    
    print("Hourly Sales Performance:")
    print(hourly_sales[['hour', 'total_transactions', 'total_revenue', 'avg_transaction_value']])
    
    # Find best and worst performing hours
    best_hour_revenue = hourly_sales.loc[hourly_sales['total_revenue'].idxmax()]
    worst_hour_revenue = hourly_sales.loc[hourly_sales['total_revenue'].idxmin()]
    
    best_hour_transactions = hourly_sales.loc[hourly_sales['total_transactions'].idxmax()]
    worst_hour_transactions = hourly_sales.loc[hourly_sales['total_transactions'].idxmin()]
    
    best_hour_avg_value = hourly_sales.loc[hourly_sales['avg_transaction_value'].idxmax()]
    worst_hour_avg_value = hourly_sales.loc[hourly_sales['avg_transaction_value'].idxmin()]
    
    print("\n=== 🏆 Hourly Performance Highlights ===")
    print("HIGHEST PERFORMERS:")
    print(f"💰 Best Revenue Hour: {int(best_hour_revenue['hour']):02d}:00 (SGD ${best_hour_revenue['total_revenue']:,.2f})")
    print(f"📊 Most Transactions Hour: {int(best_hour_transactions['hour']):02d}:00 ({int(best_hour_transactions['total_transactions']):,} transactions)")
    print(f"🎯 Best Avg Transaction Hour: {int(best_hour_avg_value['hour']):02d}:00 (SGD ${best_hour_avg_value['avg_transaction_value']:.2f})")
    print(f"👥 Most Customers Hour: {int(hourly_sales.loc[hourly_sales['unique_customers'].idxmax()]['hour']):02d}:00 ({int(hourly_sales['unique_customers'].max()):,} customers)")
    
    print("\nLOWEST PERFORMERS:")
    print(f"💸 Worst Revenue Hour: {int(worst_hour_revenue['hour']):02d}:00 (SGD ${worst_hour_revenue['total_revenue']:,.2f})")
    print(f"📉 Fewest Transactions Hour: {int(worst_hour_transactions['hour']):02d}:00 ({int(worst_hour_transactions['total_transactions']):,} transactions)")
    print(f"🎯 Lowest Avg Transaction Hour: {int(worst_hour_avg_value['hour']):02d}:00 (SGD ${worst_hour_avg_value['avg_transaction_value']:.2f})")
    print(f"👥 Fewest Customers Hour: {int(hourly_sales.loc[hourly_sales['unique_customers'].idxmin()]['hour']):02d}:00 ({int(hourly_sales['unique_customers'].min()):,} customers)")
    
    # Time period analysis
    print("\n=== 📅 Time Period Analysis ===")
    
    # Categorize hours into time periods
    def categorize_hour(hour):
        if 6 <= hour < 12:
            return 'Morning (06:00-11:59)'
        elif 12 <= hour < 18:
            return 'Afternoon (12:00-17:59)'
        elif 18 <= hour < 22:
            return 'Evening (18:00-21:59)'
        else:
            return 'Late Night (22:00-05:59)'
    
    hourly_sales['time_period'] = hourly_sales['hour'].apply(categorize_hour)
    
    period_analysis = hourly_sales.groupby('time_period').agg({
        'total_transactions': 'sum',
        'total_revenue': 'sum',
        'avg_transaction_value': 'mean',
        'unique_customers': 'sum'
    }).round(2)
    
    period_analysis = period_analysis.sort_values('total_revenue', ascending=False)
    
    print("Sales Performance by Time Period:")
    print(period_analysis)
    
    print("\n=== 🕐 Time Period Highlights ===")
    best_period = period_analysis.index[0]
    worst_period = period_analysis.index[-1]
    
    print(f"🥇 Best Time Period: {best_period}")
    print(f"   💰 Revenue: SGD ${period_analysis.loc[best_period, 'total_revenue']:,.2f}")
    print(f"   📊 Transactions: {int(period_analysis.loc[best_period, 'total_transactions']):,}")
    print(f"   🎯 Avg Transaction: SGD ${period_analysis.loc[best_period, 'avg_transaction_value']:.2f}")
    
    print(f"\n🥉 Lowest Time Period: {worst_period}")
    print(f"   💸 Revenue: SGD ${period_analysis.loc[worst_period, 'total_revenue']:,.2f}")
    print(f"   📉 Transactions: {int(period_analysis.loc[worst_period, 'total_transactions']):,}")
    print(f"   🎯 Avg Transaction: SGD ${period_analysis.loc[worst_period, 'avg_transaction_value']:.2f}")
    
    # Peak hours analysis
    print("\n=== ⏰ Peak Hours Insights ===")
    top_3_hours = hourly_sales.nlargest(3, 'total_revenue')
    bottom_3_hours = hourly_sales.nsmallest(3, 'total_revenue')
    
    print("TOP 3 REVENUE HOURS:")
    for i, (_, row) in enumerate(top_3_hours.iterrows(), 1):
        print(f"   {i}. {int(row['hour']):02d}:00 - SGD ${row['total_revenue']:,.2f} ({int(row['total_transactions']):,} transactions)")
    
    print("\nBOTTOM 3 REVENUE HOURS:")
    for i, (_, row) in enumerate(bottom_3_hours.iterrows(), 1):
        print(f"   {i}. {int(row['hour']):02d}:00 - SGD ${row['total_revenue']:,.2f} ({int(row['total_transactions']):,} transactions)")
    
    # Business recommendations based on hourly analysis
    print("\n=== 💡 Hourly Performance Recommendations ===")
    print("🎯 STAFFING OPTIMIZATION:")
    print(f"   • Peak hours ({int(best_hour_revenue['hour']):02d}:00-{int(best_hour_revenue['hour'])+1:02d}:00): Increase staff for maximum revenue capture")
    print(f"   • Low hours ({int(worst_hour_revenue['hour']):02d}:00-{int(worst_hour_revenue['hour'])+1:02d}:00): Minimal staffing or maintenance time")
    
    print("\n📢 MARKETING INSIGHTS:")
    print(f"   • Best time for promotions: {best_period}")
    print(f"   • Avoid marketing during: {worst_period}")
    
    # Calculate hourly revenue percentage
    total_daily_revenue = hourly_sales['total_revenue'].sum()
    peak_hour_percentage = (best_hour_revenue['total_revenue'] / total_daily_revenue) * 100
    low_hour_percentage = (worst_hour_revenue['total_revenue'] / total_daily_revenue) * 100
    
    print("\n📊 REVENUE CONCENTRATION:")
    print(f"   • Peak hour contributes {peak_hour_percentage:.2f}% of daily revenue")
    print(f"   • Lowest hour contributes {low_hour_percentage:.2f}% of daily revenue")
    print(f"   • Revenue variation: {peak_hour_percentage/low_hour_percentage:.1f}x difference between peak and low")

    print("\n=== Sample with Human-Readable Names and Country ===")
    sample = con.execute("""
        SELECT 
            date,
            total_amount_per_product_sgd,
            country,
            day_of_week_text as day,
            month_text as month,
            EXTRACT(year FROM date) as year
        FROM sales_data 
        WHERE total_amount_per_product_sgd >= 20
        ORDER BY total_amount_per_product_sgd DESC
        LIMIT 10
    """).df()
    print(sample)
    
    print("\n=== Sales Summary (Excluding Refunds & Exchanges) ===")
    sales_only_summary = con.execute("""
        SELECT 
            country,
            SUM(total_amount_per_product_sgd) as total_sales,
            COUNT(*) as transaction_count,
            AVG(total_amount_per_product_sgd) as avg_transaction_value,
            COUNT(DISTINCT customer_number) as unique_customers
        FROM sales_data
        WHERE transaction_desc = 'Product Sale'
        GROUP BY country
        ORDER BY total_sales DESC
    """).df()
    
    print("Sales Only Summary by Country:")
    print(sales_only_summary)
    
    # Transaction type breakdown
    transaction_breakdown = con.execute("""
        SELECT 
            transaction_desc,
            COUNT(*) as transaction_count,
            SUM(total_amount_per_product_sgd) as total_amount,
            AVG(total_amount_per_product_sgd) as avg_amount,
            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sales_data)), 2) as percentage
        FROM sales_data
        GROUP BY transaction_desc
        ORDER BY transaction_count DESC
    """).df()
    
    print("\n=== Transaction Type Breakdown ===")
    print(transaction_breakdown)
    
    sales_revenue = sales_only_summary['total_sales'].sum()
    sales_transactions = sales_only_summary['transaction_count'].sum()
    
    print("\n=== 📊 Sales Performance (Excluding Refunds & Exchanges) ===")
    print(f"💰 Total Sales Revenue: SGD ${sales_revenue:,.2f}")
    print(f"📊 Total Sales Transactions: {sales_transactions:,}")
    print(f"🎯 Average Sales Transaction: SGD ${sales_revenue/sales_transactions:.2f}")
    
    # Calculate impact of refunds and exchanges
    total_revenue = con.execute("SELECT SUM(total_amount_per_product_sgd) FROM sales_data").fetchone()[0]
    total_transactions = con.execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]
    
    refund_impact = total_revenue - sales_revenue
    transaction_impact = total_transactions - sales_transactions
    
    print("\n=== 📉 Impact of Refunds & Exchanges ===")
    print(f"💸 Revenue Impact: SGD ${refund_impact:,.2f} ({(refund_impact/total_revenue)*100:.2f}% of total)")
    print(f"📉 Transaction Impact: {transaction_impact:,} transactions ({(transaction_impact/total_transactions)*100:.2f}% of total)")

    print("\n=== 📊 Generating Visualizations ===")
    
    # Set up the plotting style
    plt.style.use('default')
    fig = plt.figure(figsize=(24, 30))
    
    # 1. Demographics Pie Charts
    print("Creating demographic pie charts...")
    
    # Age group distribution pie chart
    age_group_data = con.execute("""
        SELECT 
            CASE 
                WHEN age < 25 THEN 'Under 25'
                WHEN age BETWEEN 25 AND 40 THEN '25-40'
                WHEN age >= 41 THEN '41+'
                ELSE 'Unknown'
            END as age_group,
            COUNT(*) as customer_count
        FROM sales_data 
        WHERE age IS NOT NULL AND transaction_desc = 'Product Sale'
        GROUP BY age_group
        ORDER BY customer_count DESC
    """).df()
    
    plt.subplot(4, 3, 1)
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
    plt.pie(age_group_data['customer_count'], labels=age_group_data['age_group'], 
            autopct='%1.1f%%', startangle=90, colors=colors[:len(age_group_data)])
    plt.title('Customer Distribution by Age Group\n(Sales Only)', fontsize=12, fontweight='bold')
    
    # Country distribution pie chart
    country_customer_data = con.execute("""
        SELECT 
            country,
            COUNT(DISTINCT customer_number) as unique_customers
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY country
        ORDER BY unique_customers DESC
    """).df()
    
    plt.subplot(4, 3, 2)
    colors = plt.cm.Set3(np.linspace(0, 1, len(country_customer_data)))
    plt.pie(country_customer_data['unique_customers'], labels=country_customer_data['country'], 
            autopct='%1.1f%%', startangle=90, colors=colors)
    plt.title('Customer Distribution by Country\n(Sales Only)', fontsize=12, fontweight='bold')
    
    # Transaction type pie chart
    plt.subplot(4, 3, 3)
    colors = ['#2ECC71', '#E74C3C', '#F39C12']
    plt.pie(transaction_breakdown['transaction_count'], labels=transaction_breakdown['transaction_desc'], 
            autopct='%1.1f%%', startangle=90, colors=colors)
    plt.title('Transaction Type Distribution', fontsize=12, fontweight='bold')
    
    # 2. Yearly Sales Analysis
    print("Creating yearly sales analysis...")
    
    # Yearly sales per product
    yearly_product_sales = con.execute("""
        SELECT 
            EXTRACT(year FROM date) as year,
            product_name,
            SUM(total_amount_per_product_sgd) as total_sales,
            COUNT(*) as transaction_count
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY EXTRACT(year FROM date), product_name
        ORDER BY year, total_sales DESC
    """).df()
    
    # Get top 5 products by total sales for better visualization
    top_products = con.execute("""
        SELECT 
            product_name,
            SUM(total_amount_per_product_sgd) as total_sales
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY product_name
        ORDER BY total_sales DESC
        LIMIT 5
    """).df()
    
    # Create yearly sales chart for top products
    plt.subplot(4, 3, 4)
    for i, product in enumerate(top_products['product_name']):
        product_data = yearly_product_sales[yearly_product_sales['product_name'] == product]
        plt.plot(product_data['year'], product_data['total_sales'] / 1000000, 
                marker='o', label=product, linewidth=2)
    
    plt.title('Yearly Sales by Top 5 Products\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Year')
    plt.ylabel('Sales (Millions SGD)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    
    # Bar chart: Recent years (2020-2025) product sales
    recent_years_data = yearly_product_sales[yearly_product_sales['year'] >= 2020]
    recent_top_products = recent_years_data.groupby('product_name')['total_sales'].sum().nlargest(10)
    
    plt.subplot(4, 3, 5)
    recent_product_totals = recent_years_data.groupby('product_name')['total_sales'].sum()
    top_recent = recent_product_totals.nlargest(8)
    
    bars = plt.bar(range(len(top_recent)), top_recent.values / 1000000, 
                   color=plt.cm.viridis(np.linspace(0, 1, len(top_recent))))
    plt.title('Top Product Sales (2020-2025)\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Products')
    plt.ylabel('Sales (Millions SGD)')
    plt.xticks(range(len(top_recent)), top_recent.index, rotation=45, ha='right')
    
    # Add value labels on bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'${height:.1f}M', ha='center', va='bottom', fontsize=8)
    
    # 3. Yearly Sales by Country
    print("Creating yearly sales by country analysis...")
    
    yearly_country_sales = con.execute("""
        SELECT 
            EXTRACT(year FROM date) as year,
            country,
            SUM(total_amount_per_product_sgd) as total_sales
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY EXTRACT(year FROM date), country
        ORDER BY year, total_sales DESC
    """).df()
    
    # Top 5 countries by total sales
    top_countries = con.execute("""
        SELECT 
            country,
            SUM(total_amount_per_product_sgd) as total_sales
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY country
        ORDER BY total_sales DESC
        LIMIT 5
    """).df()
    
    # Line chart for top countries over time
    plt.subplot(4, 3, 6)
    for i, country in enumerate(top_countries['country']):
        country_data = yearly_country_sales[yearly_country_sales['country'] == country]
        plt.plot(country_data['year'], country_data['total_sales'] / 1000000, 
                marker='s', label=country, linewidth=2)
    
    plt.title('Yearly Sales by Top 5 Countries\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Year')
    plt.ylabel('Sales (Millions SGD)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    
    # 4. Yearly Sales per Product per Country (Heatmap style)
    print("Creating product-country sales analysis...")
    
    # Recent years product-country analysis
    product_country_recent = con.execute("""
        SELECT 
            country,
            product_name,
            SUM(total_amount_per_product_sgd) as total_sales
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale' 
        AND EXTRACT(year FROM date) >= 2020
        GROUP BY country, product_name
        ORDER BY total_sales DESC
    """).df()
    
    # Create pivot table for heatmap
    pivot_data = product_country_recent.pivot_table(
        index='product_name', 
        columns='country', 
        values='total_sales', 
        fill_value=0
    )
    
    # Select top 8 products and top 6 countries for better visualization
    top_8_products = product_country_recent.groupby('product_name')['total_sales'].sum().nlargest(8).index
    top_6_countries = product_country_recent.groupby('country')['total_sales'].sum().nlargest(6).index
    
    pivot_subset = pivot_data.loc[top_8_products, top_6_countries]
    
    plt.subplot(4, 3, 7)
    im = plt.imshow(pivot_subset.values / 1000000, cmap='YlOrRd', aspect='auto')
    plt.title('Product Sales by Country (2020-2025)\n(Millions SGD)', fontsize=12, fontweight='bold')
    plt.xlabel('Countries')
    plt.ylabel('Products')
    plt.xticks(range(len(pivot_subset.columns)), pivot_subset.columns, rotation=45, ha='right')
    plt.yticks(range(len(pivot_subset.index)), pivot_subset.index)
    
    # Add colorbar
    cbar = plt.colorbar(im, shrink=0.6)
    cbar.set_label('Sales (Millions SGD)', rotation=270, labelpad=15)
    
    # 5. Monthly Sales Trends
    monthly_sales = con.execute("""
        SELECT 
            month_text,
            month,
            SUM(total_amount_per_product_sgd) as total_sales,
            COUNT(*) as transaction_count
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY month_text, month
        ORDER BY month
    """).df()
    
    plt.subplot(4, 3, 8)
    bars = plt.bar(monthly_sales['month_text'], monthly_sales['total_sales'] / 1000000,
                   color=plt.cm.coolwarm(np.linspace(0, 1, len(monthly_sales))))
    plt.title('Monthly Sales Distribution\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Month')
    plt.ylabel('Sales (Millions SGD)')
    plt.xticks(rotation=45)
    
    # Add value labels
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'${height:.0f}M', ha='center', va='bottom', fontsize=8)
    
    # 6. Day of Week Analysis
    dow_sales = con.execute("""
        SELECT 
            day_of_week_text,
            day_of_week,
            SUM(total_amount_per_product_sgd) as total_sales,
            COUNT(*) as transaction_count
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY day_of_week_text, day_of_week
        ORDER BY day_of_week
    """).df()
    
    plt.subplot(4, 3, 9)
    bars = plt.bar(dow_sales['day_of_week_text'], dow_sales['total_sales'] / 1000000,
                   color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3', '#54A0FF'])
    plt.title('Sales by Day of Week\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Day of Week')
    plt.ylabel('Sales (Millions SGD)')
    
    # Add value labels
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'${height:.0f}M', ha='center', va='bottom', fontsize=8)
    
    # 7. Age Group Revenue Distribution by Country
    age_country_revenue = con.execute("""
        SELECT 
            country,
            CASE 
                WHEN age < 25 THEN 'Under 25'
                WHEN age BETWEEN 25 AND 40 THEN '25-40'
                WHEN age >= 41 THEN '41+'
                ELSE 'Unknown'
            END as age_group,
            SUM(total_amount_per_product_sgd) as total_revenue
        FROM sales_data 
        WHERE age IS NOT NULL AND transaction_desc = 'Product Sale'
        GROUP BY country, age_group
        ORDER BY country, age_group
    """).df()
    
    plt.subplot(4, 3, 10)
    age_pivot = age_country_revenue.pivot(index='country', columns='age_group', values='total_revenue').fillna(0)
    age_pivot_millions = age_pivot / 1000000
    
    # Stacked bar chart
    bottom = np.zeros(len(age_pivot_millions.index))
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
    
    for i, age_group in enumerate(['Under 25', '25-40', '41+']):
        if age_group in age_pivot_millions.columns:
            plt.bar(range(len(age_pivot_millions.index)), age_pivot_millions[age_group], 
                   bottom=bottom, label=age_group, color=colors[i])
            bottom += age_pivot_millions[age_group]
    
    plt.title('Revenue by Age Group per Country\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Countries')
    plt.ylabel('Revenue (Millions SGD)')
    plt.xticks(range(len(age_pivot_millions.index)), age_pivot_millions.index, rotation=45, ha='right')
    plt.legend()
    
    # 8. Top Products Performance Comparison
    product_performance = con.execute("""
        SELECT 
            product_name,
            SUM(total_amount_per_product_sgd) as total_revenue,
            COUNT(*) as transaction_count,
            AVG(total_amount_per_product_sgd) as avg_transaction_value,
            COUNT(DISTINCT customer_number) as unique_customers
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY product_name
        ORDER BY total_revenue DESC
        LIMIT 10
    """).df()
    
    plt.subplot(4, 3, 11)
    x_pos = np.arange(len(product_performance))
    bars = plt.bar(x_pos, product_performance['total_revenue'] / 1000000,
                   color=plt.cm.plasma(np.linspace(0, 1, len(product_performance))))
    plt.title('Top 10 Products by Revenue\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Products')
    plt.ylabel('Revenue (Millions SGD)')
    plt.xticks(x_pos, product_performance['product_name'], rotation=45, ha='right')
    
    # Add value labels
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'${height:.1f}M', ha='center', va='bottom', fontsize=7)
    
    # 9. Customer Acquisition by Year
    yearly_customers = con.execute("""
        SELECT 
            EXTRACT(year FROM date) as year,
            COUNT(DISTINCT customer_number) as new_customers,
            SUM(total_amount_per_product_sgd) as total_revenue
        FROM sales_data 
        WHERE transaction_desc = 'Product Sale'
        GROUP BY EXTRACT(year FROM date)
        ORDER BY year
    """).df()
    
    plt.subplot(4, 3, 12)
    plt.plot(yearly_customers['year'], yearly_customers['new_customers'], 
             marker='o', linewidth=2, color='#2ECC71', label='New Customers')
    plt.title('Customer Acquisition by Year\n(Sales Only)', fontsize=12, fontweight='bold')
    plt.xlabel('Year')
    plt.ylabel('Number of Customers')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    
    # Adjust layout and save
    plt.tight_layout(pad=3.0)
    
    # Add hourly analysis chart
    fig2 = plt.figure(figsize=(15, 10))
    
    # Hourly sales analysis
    plt.subplot(2, 2, 1)
    bars = plt.bar(hourly_sales['hour'], hourly_sales['total_revenue'] / 1000000,
                   color=plt.cm.viridis(np.linspace(0, 1, len(hourly_sales))))
    plt.title('Hourly Sales Revenue Distribution\n(Sales Only)', fontsize=14, fontweight='bold')
    plt.xlabel('Hour of Day')
    plt.ylabel('Revenue (Millions SGD)')
    plt.xticks(hourly_sales['hour'])
    plt.grid(True, alpha=0.3)
    
    # Add value labels on top bars
    for bar in bars:
        height = bar.get_height()
        if height > 0:  # Only label non-zero bars
            plt.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'${height:.0f}M', ha='center', va='bottom', fontsize=8)
    
    # Hourly transaction count
    plt.subplot(2, 2, 2)
    plt.plot(hourly_sales['hour'], hourly_sales['total_transactions'], 
             marker='o', linewidth=2, color='#2ECC71', markersize=6)
    plt.title('Hourly Transaction Count\n(Sales Only)', fontsize=14, fontweight='bold')
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Transactions')
    plt.xticks(hourly_sales['hour'])
    plt.grid(True, alpha=0.3)
    
    # Average transaction value by hour
    plt.subplot(2, 2, 3)
    plt.plot(hourly_sales['hour'], hourly_sales['avg_transaction_value'], 
             marker='s', linewidth=2, color='#E74C3C', markersize=6)
    plt.title('Average Transaction Value by Hour\n(Sales Only)', fontsize=14, fontweight='bold')
    plt.xlabel('Hour of Day')
    plt.ylabel('Average Transaction Value (SGD)')
    plt.xticks(hourly_sales['hour'])
    plt.grid(True, alpha=0.3)
    
    # Time period comparison
    plt.subplot(2, 2, 4)
    bars = plt.bar(range(len(period_analysis)), period_analysis['total_revenue'] / 1000000,
                   color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'])
    plt.title('Revenue by Time Period\n(Sales Only)', fontsize=14, fontweight='bold')
    plt.xlabel('Time Period')
    plt.ylabel('Revenue (Millions SGD)')
    plt.xticks(range(len(period_analysis)), period_analysis.index, rotation=45, ha='right')
    
    # Add value labels
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 10,
                f'${height:.0f}M', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout(pad=3.0)
    plt.savefig('hourly_sales_analysis.png', dpi=300, bbox_inches='tight')
    
    plt.savefig('retail_analytics_dashboard.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("📊 Visualizations saved as 'retail_analytics_dashboard.png' and 'hourly_sales_analysis.png'")
    print("🎉 Analytics dashboard complete!")

# Connection automatically closed when exiting the with block
