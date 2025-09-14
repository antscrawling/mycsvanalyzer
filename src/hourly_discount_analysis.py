import duckdb
import matplotlib.pyplot as plt
import numpy as np

def analyze_hourly_sales():
    """Analyze sales patterns by hour of the day and discount effects"""
    
    with duckdb.connect('sales_timeseries.db', read_only=True) as con:
        print("🕐 HOURLY SALES ANALYSIS")
        print("=" * 50)
        
        # Check the new database schema
        print("\n=== Updated Database Schema ===")
        result = con.execute("DESCRIBE sales_data").fetchall()
        for row in result:
            print(f"{row[0]}: {row[1]}")
        
        # 1. Basic hourly analysis for sales only
        print("\n⏰ HOURLY SALES PATTERNS (Sales Only)")
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
        
        print(hourly_sales)
        
        # Find best and worst hours
        best_hour = hourly_sales.loc[hourly_sales['total_revenue'].idxmax()]
        worst_hour = hourly_sales.loc[hourly_sales['total_revenue'].idxmin()]
        
        print("\n🏆 BEST TIME OF DAY:")
        print(f"   🕐 Hour: {int(best_hour['hour'])}:00 ({int(best_hour['hour'])}:00-{int(best_hour['hour'])+1}:00)")
        print(f"   💰 Revenue: SGD ${best_hour['total_revenue']:,.2f}")
        print(f"   📊 Transactions: {int(best_hour['total_transactions']):,}")
        print(f"   👥 Customers: {int(best_hour['unique_customers']):,}")
        print(f"   🎯 Avg Transaction: SGD ${best_hour['avg_transaction_value']:.2f}")
        
        print("\n🥉 WORST TIME OF DAY:")
        print(f"   🕐 Hour: {int(worst_hour['hour'])}:00 ({int(worst_hour['hour'])}:00-{int(worst_hour['hour'])+1}:00)")
        print(f"   💸 Revenue: SGD ${worst_hour['total_revenue']:,.2f}")
        print(f"   📉 Transactions: {int(worst_hour['total_transactions']):,}")
        print(f"   👥 Customers: {int(worst_hour['unique_customers']):,}")
        print(f"   🎯 Avg Transaction: SGD ${worst_hour['avg_transaction_value']:.2f}")
        
        # 2. Discount period analysis
        print("\n🎁 DISCOUNT PERIOD ANALYSIS")
        discount_analysis = con.execute("""
            SELECT 
                discount_period,
                COUNT(*) as total_transactions,
                SUM(total_amount_per_product_sgd) as total_revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction_value,
                COUNT(DISTINCT customer_number) as unique_customers
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale' AND discount_applied = true
            GROUP BY discount_period
            ORDER BY total_revenue DESC
        """).df()
        
        print("Discount Period Performance:")
        print(discount_analysis)
        
        # Regular vs Discount sales comparison
        regular_vs_discount = con.execute("""
            SELECT 
                CASE WHEN discount_applied THEN 'Discount' ELSE 'Regular' END as sale_type,
                COUNT(*) as total_transactions,
                SUM(total_amount_per_product_sgd) as total_revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction_value,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sales_data WHERE transaction_desc = 'Product Sale'), 2) as percentage
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY discount_applied
            ORDER BY total_revenue DESC
        """).df()
        
        print("\n📊 REGULAR VS DISCOUNT SALES:")
        print(regular_vs_discount)
        
        # 3. Hourly analysis during discount periods
        hourly_discount = con.execute("""
            SELECT 
                hour,
                discount_period,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale' AND discount_applied = true
            GROUP BY hour, discount_period
            ORDER BY hour, revenue DESC
        """).df()
        
        if not hourly_discount.empty:
            print("\n🕐 HOURLY PATTERNS DURING DISCOUNT PERIODS:")
            # Get top performing hours during discounts
            top_discount_hours = hourly_discount.groupby('hour')['revenue'].sum().nlargest(5)
            print("Top 5 Hours During Discount Periods:")
            for hour, revenue in top_discount_hours.items():
                print(f"   🕐 {int(hour)}:00 - SGD ${revenue:,.2f}")
        
        # 4. Generate visualizations
        print("\n📊 GENERATING HOURLY VISUALIZATIONS...")
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Plot 1: Hourly revenue
        ax1.bar(hourly_sales['hour'], hourly_sales['total_revenue'] / 1000000, 
                color=plt.cm.viridis(np.linspace(0, 1, len(hourly_sales))))
        ax1.set_title('Revenue by Hour of Day (Sales Only)', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Hour of Day')
        ax1.set_ylabel('Revenue (Millions SGD)')
        ax1.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for i, (hour, revenue) in enumerate(zip(hourly_sales['hour'], hourly_sales['total_revenue'])):
            ax1.text(hour, revenue/1000000 + 1, f'${revenue/1000000:.0f}M', 
                    ha='center', va='bottom', fontsize=8, rotation=45)
        
        # Plot 2: Hourly transaction count
        ax2.plot(hourly_sales['hour'], hourly_sales['total_transactions'], 
                marker='o', linewidth=2, color='#E74C3C')
        ax2.set_title('Transaction Count by Hour', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Hour of Day')
        ax2.set_ylabel('Number of Transactions')
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Average transaction value by hour
        ax3.bar(hourly_sales['hour'], hourly_sales['avg_transaction_value'], 
                color=plt.cm.coolwarm(np.linspace(0, 1, len(hourly_sales))))
        ax3.set_title('Average Transaction Value by Hour', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Hour of Day')
        ax3.set_ylabel('Average Transaction (SGD)')
        ax3.grid(True, alpha=0.3)
        
        # Plot 4: Regular vs Discount comparison
        if len(regular_vs_discount) > 1:
            colors = ['#3498DB', '#E67E22']
            ax4.pie(regular_vs_discount['total_revenue'], 
                   labels=regular_vs_discount['sale_type'],
                   autopct='%1.1f%%', colors=colors, startangle=90)
            ax4.set_title('Revenue: Regular vs Discount Sales', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('hourly_sales_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # 5. Business insights summary
        print("\n💡 KEY INSIGHTS:")
        peak_hours = hourly_sales.nlargest(3, 'total_revenue')['hour'].values
        low_hours = hourly_sales.nsmallest(3, 'total_revenue')['hour'].values
        
        print(f"   🔥 Peak Sales Hours: {', '.join([f'{int(h)}:00' for h in peak_hours])}")
        print(f"   📉 Low Sales Hours: {', '.join([f'{int(h)}:00' for h in low_hours])}")
        
        if len(regular_vs_discount) > 1:
            discount_impact = regular_vs_discount[regular_vs_discount['sale_type'] == 'Discount']['percentage'].iloc[0]
            print(f"   🎁 Discount Sales Impact: {discount_impact}% of total transactions")
        
        total_revenue_diff = best_hour['total_revenue'] - worst_hour['total_revenue']
        print(f"   📊 Peak vs Low Hour Difference: SGD ${total_revenue_diff:,.2f}")
        
        return {
            'hourly_sales': hourly_sales,
            'best_hour': best_hour,
            'worst_hour': worst_hour,
            'discount_analysis': discount_analysis,
            'regular_vs_discount': regular_vs_discount
        }

if __name__ == "__main__":
    results = analyze_hourly_sales()
    print("\n✅ Hourly analysis complete!")
    print("📊 Visualization saved as 'hourly_sales_analysis.png'")
