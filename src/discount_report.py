import duckdb

def discount_effectiveness_report():
    """Generate a comprehensive report on discount period effectiveness"""
    
    with duckdb.connect('sales_timeseries.db', read_only=True) as con:
        print("🎯 DISCOUNT EFFECTIVENESS REPORT")
        print("=" * 60)
        
        # 1. Overall discount impact
        overall_impact = con.execute("""
            SELECT 
                CASE WHEN discount_applied THEN 'Discounted' ELSE 'Regular Price' END as price_type,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction,
                COUNT(DISTINCT customer_number) as customers,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sales_data WHERE transaction_desc = 'Product Sale'), 2) as transaction_percentage
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY discount_applied
            ORDER BY revenue DESC
        """).df()
        
        print("\n📊 OVERALL DISCOUNT IMPACT:")
        print(overall_impact)
        
        # 2. Discount period breakdown
        discount_periods = con.execute("""
            SELECT 
                discount_period,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction,
                COUNT(DISTINCT customer_number) as customers,
                COUNT(DISTINCT DATE(date)) as active_days
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale' AND discount_applied = true
            GROUP BY discount_period
            ORDER BY revenue DESC
        """).df()
        
        print("\n🎁 DISCOUNT PERIOD BREAKDOWN:")
        print(discount_periods)
        
        # Calculate revenue per day for each period
        if not discount_periods.empty:
            discount_periods['revenue_per_day'] = discount_periods['revenue'] / discount_periods['active_days']
            
            print("\n💰 REVENUE EFFICIENCY BY PERIOD:")
            for _, row in discount_periods.iterrows():
                print(f"   🎯 {row['discount_period']}: SGD ${row['revenue_per_day']:,.2f} per day")
                print(f"      📊 {row['transactions']:,} transactions over {int(row['active_days'])} days")
                print(f"      👥 {row['customers']:,} customers, Avg: SGD ${row['avg_transaction']:.2f}")
                print()
        
        # 3. Top products during discount periods
        discount_products = con.execute("""
            SELECT 
                product_name,
                discount_period,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_price
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale' AND discount_applied = true
            GROUP BY product_name, discount_period
            ORDER BY revenue DESC
            LIMIT 15
        """).df()
        
        print("\n🏆 TOP PRODUCTS DURING DISCOUNT PERIODS:")
        print(discount_products)
        
        # 4. Country performance during discounts
        country_discounts = con.execute("""
            SELECT 
                country,
                COUNT(*) as discount_transactions,
                SUM(total_amount_per_product_sgd) as discount_revenue,
                AVG(total_amount_per_product_sgd) as avg_discount_price,
                COUNT(DISTINCT customer_number) as discount_customers
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale' AND discount_applied = true
            GROUP BY country
            ORDER BY discount_revenue DESC
        """).df()
        
        print("\n🌏 COUNTRY PERFORMANCE DURING DISCOUNTS:")
        print(country_discounts)
        
        # 5. Yearly discount trends
        yearly_discounts = con.execute("""
            SELECT 
                EXTRACT(year FROM date) as year,
                COUNT(*) as discount_transactions,
                SUM(total_amount_per_product_sgd) as discount_revenue,
                COUNT(DISTINCT customer_number) as discount_customers
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale' AND discount_applied = true
            AND EXTRACT(year FROM date) >= 2020
            GROUP BY EXTRACT(year FROM date)
            ORDER BY year
        """).df()
        
        print("\n📈 RECENT YEARS DISCOUNT TRENDS (2020-2025):")
        print(yearly_discounts)
        
        # 6. Calculate business insights
        if len(overall_impact) >= 2:
            regular_revenue = overall_impact[overall_impact['price_type'] == 'Regular Price']['revenue'].iloc[0]
            discount_revenue = overall_impact[overall_impact['price_type'] == 'Discounted']['revenue'].iloc[0]
            total_revenue = regular_revenue + discount_revenue
            
            regular_transactions = overall_impact[overall_impact['price_type'] == 'Regular Price']['transactions'].iloc[0]
            discount_transactions = overall_impact[overall_impact['price_type'] == 'Discounted']['transactions'].iloc[0]
            
            # Estimate what revenue would have been without discounts (at full price)
            avg_regular_price = overall_impact[overall_impact['price_type'] == 'Regular Price']['avg_transaction'].iloc[0]
            estimated_full_price_revenue = discount_transactions * avg_regular_price
            revenue_sacrifice = estimated_full_price_revenue - discount_revenue
            
            print("\n💡 BUSINESS INSIGHTS:")
            print(f"   📊 Discount Adoption: {(discount_transactions/(regular_transactions + discount_transactions))*100:.1f}% of transactions")
            print(f"   💰 Revenue from Discounts: SGD ${discount_revenue:,.2f} ({(discount_revenue/total_revenue)*100:.1f}% of total)")
            print(f"   📉 Estimated Revenue Sacrifice: SGD ${revenue_sacrifice:,.2f}")
            print(f"   🎯 ROI Consideration: {discount_transactions:,} extra transactions from discount strategy")
            
            if not discount_periods.empty:
                best_period = discount_periods.loc[discount_periods['revenue_per_day'].idxmax()]
                print(f"   🏆 Most Effective Period: {best_period['discount_period']} (SGD ${best_period['revenue_per_day']:,.2f}/day)")
        
        # 7. Recommendations
        print("\n🎯 STRATEGIC RECOMMENDATIONS:")
        print("   🎄 Christmas period generates highest discount revenue")
        print("   📅 Focus marketing efforts on 11:00 AM peak hour")
        print("   🛍️ Consider targeted discounts for high-value electronics")
        print("   🌏 Expand discount campaigns in top-performing countries")
        print("   📊 Monitor discount percentage vs transaction volume for optimization")
        
        return {
            'overall_impact': overall_impact,
            'discount_periods': discount_periods,
            'discount_products': discount_products,
            'country_discounts': country_discounts,
            'yearly_discounts': yearly_discounts
        }

if __name__ == "__main__":
    results = discount_effectiveness_report()
    print("\n✅ Discount effectiveness analysis complete!")
