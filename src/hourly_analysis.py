import duckdb

def analyze_daily_sales():
    """Detailed analysis of sales performance by day"""
    
    with duckdb.connect('src/sales_timeseries.db', read_only=True) as con:
        print("📅 DAILY SALES PERFORMANCE ANALYSIS")
        print("=" * 60)
        
        # Get daily sales data
        daily_data = con.execute("""
            SELECT 
                DATE(date) as sale_date,
                DAYNAME(date) as day_name,
                DAYOFWEEK(date) as day_number,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_value,
                COUNT(DISTINCT customer_id) as customers,
                COUNT(DISTINCT receipt_number) as receipts
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY DATE(date), DAYNAME(date), DAYOFWEEK(date)
            ORDER BY sale_date
        """).fetchall()
        
        print("\n📊 COMPLETE DAILY BREAKDOWN:")
        print("Date       | Day        | Transactions | Revenue (SGD) | Avg Value | Customers")
        print("-" * 80)
        
        for row in daily_data:
            date, day_name, day_num, transactions, revenue, avg_value, customers, receipts = row
            print(f"{date} | {day_name:<10} | {transactions:>11,} | {revenue:>13,.0f} | {avg_value:>8.2f} | {customers:>9,}")
        
        # Convert to list of dicts for easier analysis
        data_dicts = []
        for row in daily_data:
            data_dicts.append({
                'sale_date': row[0],
                'day_name': row[1], 
                'day_number': row[2],
                'transactions': row[3],
                'revenue': float(row[4]),  # Convert Decimal to float
                'avg_value': float(row[5]),  # Convert Decimal to float
                'customers': row[6],
                'receipts': row[7]
            })
        
        # Check if we have data
        if not data_dicts:
            print("No data found!")
            return [], {}, []
        
        # Find peaks and valleys - FIXED SYNTAX ERRORS
        best_revenue_day = max(data_dicts, key=lambda x: x['revenue'])
        worst_revenue_day = min(data_dicts, key=lambda x: x['revenue'])
        
        best_transaction_day = max(data_dicts, key=lambda x: x['transactions'])
        worst_transaction_day = min(data_dicts, key=lambda x: x['transactions'])
        
        best_avg_value_day = max(data_dicts, key=lambda x: x['avg_value'])
        worst_avg_value_day = min(data_dicts, key=lambda x: x['avg_value'])
        
        print("\n🏆 PEAK PERFORMANCE DAYS:")
        print(f"💰 Highest Revenue: {best_revenue_day['sale_date']} ({best_revenue_day['day_name']})")
        print(f"   Revenue: SGD ${best_revenue_day['revenue']:,.2f}")
        print(f"   Transactions: {best_revenue_day['transactions']:,}")
        print(f"   Customers: {best_revenue_day['customers']:,}")
        
        print(f"\n📊 Most Active Day: {best_transaction_day['sale_date']} ({best_transaction_day['day_name']})")
        print(f"   Transactions: {best_transaction_day['transactions']:,}")
        print(f"   Revenue: SGD ${best_transaction_day['revenue']:,.2f}")
        
        print(f"\n💎 Highest Value Day: {best_avg_value_day['sale_date']} ({best_avg_value_day['day_name']})")
        print(f"   Average Transaction: SGD ${best_avg_value_day['avg_value']:.2f}")
        print(f"   Total Revenue: SGD ${best_avg_value_day['revenue']:,.2f}")
        
        print("\n📉 LOWEST PERFORMANCE DAYS:")
        print(f"💸 Lowest Revenue: {worst_revenue_day['sale_date']} ({worst_revenue_day['day_name']})")
        print(f"   Revenue: SGD ${worst_revenue_day['revenue']:,.2f}")
        print(f"   Transactions: {worst_revenue_day['transactions']:,}")
        
        print(f"\n📉 Least Active Day: {worst_transaction_day['sale_date']} ({worst_transaction_day['day_name']})")
        print(f"   Transactions: {worst_transaction_day['transactions']:,}")
        print(f"   Revenue: SGD ${worst_transaction_day['revenue']:,.2f}")
        
        print(f"\n💸 Lowest Value Day: {worst_avg_value_day['sale_date']} ({worst_avg_value_day['day_name']})")
        print(f"   Average Transaction: SGD ${worst_avg_value_day['avg_value']:.2f}")
        print(f"   Total Revenue: SGD ${worst_avg_value_day['revenue']:.2f}")  # FIXED: removed extra colon
        
        # Day of week analysis
        dow_summary = {}
        for row in data_dicts:
            day_name = row['day_name']
            if day_name not in dow_summary:
                dow_summary[day_name] = {
                    'transactions': 0,
                    'revenue': 0,
                    'customers': 0,
                    'days': 0
                }
            dow_summary[day_name]['transactions'] += row['transactions']
            dow_summary[day_name]['revenue'] += row['revenue']
            dow_summary[day_name]['customers'] += row['customers']
            dow_summary[day_name]['days'] += 1
        
        # Calculate averages
        for day in dow_summary:
            days_count = dow_summary[day]['days']
            dow_summary[day]['avg_transactions'] = dow_summary[day]['transactions'] / days_count
            dow_summary[day]['avg_revenue'] = dow_summary[day]['revenue'] / days_count
            dow_summary[day]['avg_customers'] = dow_summary[day]['customers'] / days_count
            dow_summary[day]['avg_value'] = dow_summary[day]['revenue'] / dow_summary[day]['transactions'] if dow_summary[day]['transactions'] > 0 else 0
        
        # Sort by average revenue
        sorted_days = sorted(dow_summary.items(), key=lambda x: x[1]['avg_revenue'], reverse=True)
        
        print("\n📅 DAY OF WEEK PERFORMANCE:")
        print("Day        | Avg Transactions | Avg Revenue (SGD) | Avg Value | Avg Customers")
        print("-" * 80)
        for day_name, stats in sorted_days:
            print(f"{day_name:<10} | {stats['avg_transactions']:>15,.1f} | {stats['avg_revenue']:>17,.0f} | {stats['avg_value']:>8.2f} | {stats['avg_customers']:>13,.1f}")
        
        # Weekly patterns analysis
        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        weekends = ['Saturday', 'Sunday']
        
        weekday_revenue = sum([dow_summary.get(day, {}).get('avg_revenue', 0) for day in weekdays]) / len(weekdays)
        weekend_revenue = sum([dow_summary.get(day, {}).get('avg_revenue', 0) for day in weekends]) / len(weekends)
        
        weekday_transactions = sum([dow_summary.get(day, {}).get('avg_transactions', 0) for day in weekdays]) / len(weekdays)
        weekend_transactions = sum([dow_summary.get(day, {}).get('avg_transactions', 0) for day in weekends]) / len(weekends)
        
        print("\n📈 WEEKDAY vs WEEKEND COMPARISON:")
        print(f"🏢 Weekday Average Revenue: SGD ${weekday_revenue:,.0f}")
        print(f"🎯 Weekend Average Revenue: SGD ${weekend_revenue:,.0f}")
        
        # Avoid division by zero
        if weekday_revenue > 0:
            print(f"📊 Weekend vs Weekday Revenue: {(weekend_revenue/weekday_revenue-1)*100:+.1f}%")
        if weekday_transactions > 0:
            print(f"🏢 Weekday Average Transactions: {weekday_transactions:,.1f}")
            print(f"🎯 Weekend Average Transactions: {weekend_transactions:,.1f}")
            print(f"📊 Weekend vs Weekday Transactions: {(weekend_transactions/weekday_transactions-1)*100:+.1f}%")
        
        # Business insights
        total_revenue = sum([row['revenue'] for row in data_dicts])
        peak_revenue_pct = (best_revenue_day['revenue'] / total_revenue) * 100
        low_revenue_pct = (worst_revenue_day['revenue'] / total_revenue) * 100
        
        print("\n💡 BUSINESS INSIGHTS:")
        print(f"🎯 Peak day ({best_revenue_day['day_name']}) generates {peak_revenue_pct:.2f}% of total revenue")
        print(f"🎯 Low day ({worst_revenue_day['day_name']}) generates {low_revenue_pct:.2f}% of total revenue")
        print(f"🎯 Revenue variation: {peak_revenue_pct/low_revenue_pct:.2f}x difference between peak and low")
        
        # Top and bottom performing days
        top_5 = sorted(data_dicts, key=lambda x: x['revenue'], reverse=True)[:5]
        bottom_5 = sorted(data_dicts, key=lambda x: x['revenue'])[:5]
        
        print("\n🥇 TOP 5 REVENUE DAYS:")
        for i, day in enumerate(top_5, 1):
            print(f"   {i}. {day['sale_date']} ({day['day_name']}) - SGD ${day['revenue']:,.0f} ({day['transactions']:,} transactions)")
        
        print("\n🥉 BOTTOM 5 REVENUE DAYS:")
        for i, day in enumerate(bottom_5, 1):
            print(f"   {i}. {day['sale_date']} ({day['day_name']}) - SGD ${day['revenue']:,.0f} ({day['transactions']:,} transactions)")
        
        # Month trends if data spans multiple months
        monthly_trends = con.execute("""
            SELECT 
                YEAR(date) as year,
                MONTH(date) as month,
                MONTHNAME(date) as month_name,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_value,
                COUNT(DISTINCT customer_id) as customers
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY YEAR(date), MONTH(date), MONTHNAME(date)
            ORDER BY year, month
        """).fetchall()
        
        if len(monthly_trends) > 1:
            print("\n📊 MONTHLY TRENDS:")
            print("Month      | Transactions | Revenue (SGD) | Avg Value | Customers")
            print("-" * 70)
            for row in monthly_trends:
                year, month, month_name, transactions, revenue, avg_value, customers = row
                print(f"{month_name} {year} | {transactions:>11,} | {revenue:>13,.0f} | {avg_value:>8.2f} | {customers:>9,}")
        
        # Recommendations
        print("\n🎯 STRATEGIC RECOMMENDATIONS:")
        print("📈 Peak Performance:")
        best_day_name = sorted_days[0][0]
        print(f"   • Focus marketing campaigns on {best_day_name}s")
        print(f"   • Ensure full staffing on {best_day_name}s")
        print(f"   • Schedule premium product launches on {best_day_name}s")
        
        print("\n📉 Optimization Opportunities:")
        worst_day_name = sorted_days[-1][0]
        print(f"   • Consider promotional campaigns on {worst_day_name}s")
        print(f"   • Use {worst_day_name}s for staff training and maintenance")
        print(f"   • Implement special {worst_day_name} discounts to boost sales")
        
        if weekend_revenue > weekday_revenue:
            print("\n🎯 Weekend Focus Strategy:")
            print("   • Weekend shoppers drive higher revenue - maintain strong weekend presence")
            print("   • Consider extending weekend hours")
        else:
            print("\n🎯 Weekday Focus Strategy:")
            print("   • Weekday business is stronger - optimize weekday operations")
            print("   • Consider weekend promotional events")
        
        return data_dicts, dow_summary, monthly_trends

if __name__ == "__main__":
    daily_data, dow_summary, monthly_trends = analyze_daily_sales()
    print("\n✅ Daily analysis complete!")
    print(f"📊 Data covers {len(daily_data)} days with detailed insights.")