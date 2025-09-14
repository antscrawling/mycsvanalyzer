import duckdb

def analyze_hourly_sales():
    """Detailed analysis of sales performance by hour of day"""
    
    with duckdb.connect('sales_timeseries.db', read_only=True) as con:
        print("🕐 HOURLY SALES PERFORMANCE ANALYSIS")
        print("=" * 60)
        
        # Get hourly sales data
        hourly_data = con.execute("""
            SELECT 
                hour,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_value,
                COUNT(DISTINCT customer_number) as customers,
                COUNT(DISTINCT receipt_number) as receipts
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY hour
            ORDER BY hour
        """).df()
        
        print("\n📊 COMPLETE HOURLY BREAKDOWN:")
        print("Hour | Transactions | Revenue (SGD) | Avg Value | Customers")
        print("-" * 65)
        for _, row in hourly_data.iterrows():
            print(f"{int(row['hour']):02d}:00 | {int(row['transactions']):>11,} | {row['revenue']:>13,.0f} | {row['avg_value']:>8.2f} | {int(row['customers']):>9,}")
        
        # Find peaks and valleys
        best_revenue_hour = hourly_data.loc[hourly_data['revenue'].idxmax()]
        worst_revenue_hour = hourly_data.loc[hourly_data['revenue'].idxmin()]
        
        best_transaction_hour = hourly_data.loc[hourly_data['transactions'].idxmax()]
        worst_transaction_hour = hourly_data.loc[hourly_data['transactions'].idxmin()]
        
        best_avg_value_hour = hourly_data.loc[hourly_data['avg_value'].idxmax()]
        worst_avg_value_hour = hourly_data.loc[hourly_data['avg_value'].idxmin()]
        
        print("\n🏆 PEAK PERFORMANCE HOURS:")
        print(f"💰 Highest Revenue: {int(best_revenue_hour['hour']):02d}:00")
        print(f"   Revenue: SGD ${best_revenue_hour['revenue']:,.2f}")
        print(f"   Transactions: {int(best_revenue_hour['transactions']):,}")
        print(f"   Customers: {int(best_revenue_hour['customers']):,}")
        
        print(f"\n📊 Most Active Hour: {int(best_transaction_hour['hour']):02d}:00")
        print(f"   Transactions: {int(best_transaction_hour['transactions']):,}")
        print(f"   Revenue: SGD ${best_transaction_hour['revenue']:,.2f}")
        
        print(f"\n💎 Highest Value Hour: {int(best_avg_value_hour['hour']):02d}:00")
        print(f"   Average Transaction: SGD ${best_avg_value_hour['avg_value']:.2f}")
        print(f"   Total Revenue: SGD ${best_avg_value_hour['revenue']:,.2f}")
        
        print("\n📉 LOWEST PERFORMANCE HOURS:")
        print(f"💸 Lowest Revenue: {int(worst_revenue_hour['hour']):02d}:00")
        print(f"   Revenue: SGD ${worst_revenue_hour['revenue']:,.2f}")
        print(f"   Transactions: {int(worst_revenue_hour['transactions']):,}")
        
        print(f"\n📉 Least Active Hour: {int(worst_transaction_hour['hour']):02d}:00")
        print(f"   Transactions: {int(worst_transaction_hour['transactions']):,}")
        print(f"   Revenue: SGD ${worst_transaction_hour['revenue']:,.2f}")
        
        print(f"\n💸 Lowest Value Hour: {int(worst_avg_value_hour['hour']):02d}:00")
        print(f"   Average Transaction: SGD ${worst_avg_value_hour['avg_value']:.2f}")
        print(f"   Total Revenue: SGD ${worst_avg_value_hour['revenue']:,.2f}")
        
        # Time period analysis
        def get_time_period(hour):
            if 6 <= hour < 9:
                return 'Early Morning'
            elif 9 <= hour < 12:
                return 'Late Morning'
            elif 12 <= hour < 15:
                return 'Early Afternoon'
            elif 15 <= hour < 18:
                return 'Late Afternoon'
            elif 18 <= hour < 21:
                return 'Evening'
            else:
                return 'Night'
        
        hourly_data['period'] = hourly_data['hour'].apply(get_time_period)
        period_summary = hourly_data.groupby('period').agg({
            'transactions': 'sum',
            'revenue': 'sum',
            'avg_value': 'mean',
            'customers': 'sum'
        }).round(2)
        
        period_summary = period_summary.sort_values('revenue', ascending=False)
        
        print("\n⏰ TIME PERIOD PERFORMANCE:")
        print("Period | Transactions | Revenue (SGD) | Avg Value | Customers")
        print("-" * 70)
        for period, row in period_summary.iterrows():
            print(f"{period:<15} | {int(row['transactions']):>11,} | {row['revenue']:>13,.0f} | {row['avg_value']:>8.2f} | {int(row['customers']):>9,}")
        
        # Business insights
        total_revenue = hourly_data['revenue'].sum()
        peak_revenue_pct = (best_revenue_hour['revenue'] / total_revenue) * 100
        low_revenue_pct = (worst_revenue_hour['revenue'] / total_revenue) * 100
        
        print("\n💡 BUSINESS INSIGHTS:")
        print(f"🎯 Peak hour ({int(best_revenue_hour['hour']):02d}:00) generates {peak_revenue_pct:.2f}% of daily revenue")
        print(f"🎯 Low hour ({int(worst_revenue_hour['hour']):02d}:00) generates {low_revenue_pct:.2f}% of daily revenue")
        print(f"🎯 Revenue variation: {peak_revenue_pct/low_revenue_pct:.2f}x difference between peak and low")
        
        # Operating hours recommendation
        operating_hours = hourly_data[(hourly_data['hour'] >= 6) & (hourly_data['hour'] <= 21)]
        total_operating_revenue = operating_hours['revenue'].sum()
        operating_revenue_pct = (total_operating_revenue / total_revenue) * 100
        
        print("\n🏪 OPERATING HOURS ANALYSIS:")
        print(f"🕕 Store Hours (06:00-21:59): {operating_revenue_pct:.1f}% of total revenue")
        print(f"🌙 Night Hours (22:00-05:59): {100-operating_revenue_pct:.1f}% of total revenue")
        
        # Top 3 and bottom 3 hours
        top_3 = hourly_data.nlargest(3, 'revenue')
        bottom_3 = hourly_data.nsmallest(3, 'revenue')
        
        print("\n🥇 TOP 3 REVENUE HOURS:")
        for i, (_, row) in enumerate(top_3.iterrows(), 1):
            print(f"   {i}. {int(row['hour']):02d}:00 - SGD ${row['revenue']:,.0f} ({int(row['transactions']):,} transactions)")
        
        print("\n🥉 BOTTOM 3 REVENUE HOURS:")
        for i, (_, row) in enumerate(bottom_3.iterrows(), 1):
            print(f"   {i}. {int(row['hour']):02d}:00 - SGD ${row['revenue']:,.0f} ({int(row['transactions']):,} transactions)")
        
        # Recommendations
        print("\n🎯 STRATEGIC RECOMMENDATIONS:")
        print("📈 Peak Performance:")
        print(f"   • Schedule more staff during {int(best_revenue_hour['hour']):02d}:00-{int(best_revenue_hour['hour'])+1:02d}:00")
        print("   • Focus premium promotions during morning hours")
        print(f"   • Ensure full inventory during {int(best_transaction_hour['hour']):02d}:00-{int(best_transaction_hour['hour'])+1:02d}:00")
        
        print("\n📉 Optimization Opportunities:")
        print(f"   • Consider reduced staffing during {int(worst_revenue_hour['hour']):02d}:00-{int(worst_revenue_hour['hour'])+1:02d}:00")
        print("   • Use evening hours for maintenance and restocking")
        print("   • Implement evening discounts to boost sales")
        
        return hourly_data, period_summary

if __name__ == "__main__":
    hourly_data, period_summary = analyze_hourly_sales()
    print("\n✅ Hourly analysis complete!")
    print(f"📊 Data covers {len(hourly_data)} operating hours with detailed insights.")
