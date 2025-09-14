import duckdb
import os

def test_system():
    """Test the retail analytics system"""
    print("🧪 TESTING RETAIL ANALYTICS SYSTEM")
    print("=" * 40)
    
    # Test 1: Database exists and accessible
    print("\n1️⃣ Testing database connection...")
    try:
        with duckdb.connect('sales_timeseries.db', read_only=True) as con:
            count = con.execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]
            print(f"   ✅ Database accessible: {count:,} records")
    except Exception as e:
        print(f"   ❌ Database error: {e}")
        return False
    
    # Test 2: Check for required files
    print("\n2️⃣ Testing required files...")
    required_files = [
        'main.py',
        'retail_menu.py',
        'hourly_discount_analysis.py',
        'discount_report.py',
        'final_summary.py',
        'visualization_insights.py'
    ]
    
    for file in required_files:
        if os.path.exists(file):
            print(f"   ✅ {file} found")
        else:
            print(f"   ❌ {file} missing")
    
    # Test 3: Quick database statistics
    print("\n3️⃣ Testing database content...")
    try:
        with duckdb.connect('sales_timeseries.db', read_only=True) as con:
            stats = con.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT customer_number) as customers,
                    COUNT(DISTINCT product_name) as products,
                    COUNT(DISTINCT country) as countries,
                    SUM(CASE WHEN discount_applied THEN 1 ELSE 0 END) as discount_transactions
                FROM sales_data
            """).fetchone()
            
            print(f"   📊 Total Records: {stats[0]:,}")
            print(f"   👥 Customers: {stats[1]:,}")
            print(f"   🛍️ Products: {stats[2]}")
            print(f"   🌏 Countries: {stats[3]}")
            print(f"   🎁 Discount Transactions: {stats[4]:,}")
            
            if stats[0] > 3000000:
                print("   ✅ Database has sufficient data")
            else:
                print("   ⚠️ Database may need regeneration")
                
    except Exception as e:
        print(f"   ❌ Database content error: {e}")
    
    # Test 4: Check discount periods functionality
    print("\n4️⃣ Testing discount periods...")
    try:
        with duckdb.connect('sales_timeseries.db', read_only=True) as con:
            discount_periods = con.execute("""
                SELECT discount_period, COUNT(*) as count
                FROM sales_data 
                WHERE discount_applied = true
                GROUP BY discount_period
                ORDER BY count DESC
            """).fetchall()
            
            if discount_periods:
                print("   ✅ Discount periods working:")
                for period, count in discount_periods:
                    print(f"      🎁 {period}: {count:,} transactions")
            else:
                print("   ❌ No discount periods found")
                
    except Exception as e:
        print(f"   ❌ Discount periods error: {e}")
    
    # Test 5: Sample export functionality
    print("\n5️⃣ Testing export functionality...")
    try:
        with duckdb.connect('sales_timeseries.db', read_only=True) as con:
            sample_df = con.execute("""
                SELECT * FROM sales_data 
                WHERE transaction_desc = 'Product Sale'
                LIMIT 100
            """).df()
            
            test_filename = "test_export.csv"
            sample_df.to_csv(test_filename, index=False)
            
            if os.path.exists(test_filename):
                print(f"   ✅ Export working: created {test_filename}")
                os.remove(test_filename)  # Clean up
            else:
                print("   ❌ Export failed")
                
    except Exception as e:
        print(f"   ❌ Export error: {e}")
    
    print("\n" + "=" * 40)
    print("🎉 SYSTEM TEST COMPLETE!")
    print("\n💡 To start the menu system, run:")
    print("   .venv/bin/python retail_menu.py")
    print("\n📖 For detailed instructions, see:")
    print("   QUICK_START_GUIDE.md")

if __name__ == "__main__":
    test_system()
