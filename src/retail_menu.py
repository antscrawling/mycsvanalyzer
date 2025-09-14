import os
import sys
import duckdb
from datetime import datetime

class RetailMenu:
    def clear_screen(self):
        """Clear the terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')

    def display_menu(self):
        """Display the main menu"""
        self.clear_screen()
        print("🏪 RETAIL ANALYTICS SYSTEM")
        print("=" * 50)
        print("📅", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 50)
        print()
        print("📋 MAIN MENU:")
        print("   1️⃣  Recreate Database (Generate Initial Data)")
        print("   2️⃣  Hourly Sales Analysis")
        print("   3️⃣  Discount Effectiveness Report") 
        print("   4️⃣  Complete Retail Analytics Dashboard")
        print("   5️⃣  Final Summary Report")
        print("   6️⃣  Export All Data to CSV")
        print("   7️⃣  Database Schema & Info")
        print("   8️⃣  Quick Business Insights")
        print("   9️⃣  Generate All Visualizations")
        print("   0️⃣  Exit")
        print("=" * 50)

    def recreate_database(self):
        """Option 1: Recreate the database"""
        print("🔄 RECREATING DATABASE...")
        print("=" * 30)
        print("⚠️  This will delete existing data and create a new database.")
        confirm = input("Are you sure? (y/N): ").lower()
        
        if confirm == 'y':
            print("\n📊 Starting database generation...")
            try:
                # Import and run the main data generation
                from main import generate_initial_data
                generate_initial_data()
                print("\n✅ Database recreation completed successfully!")
            except Exception as e:
                print(f"\n❌ Error during database creation: {e}")
        else:
            print("❌ Database recreation cancelled.")
        
        input("\nPress Enter to continue...")

    def hourly_analysis(self):
        """Option 2: Run hourly sales analysis"""
        print("🕐 HOURLY SALES ANALYSIS")
        print("=" * 30)
        try:
            print("📊 Running hourly analysis...")
            exec(open('hourly_discount_analysis.py').read())
            print("\n✅ Hourly analysis completed!")
        except Exception as e:
            print(f"❌ Error running hourly analysis: {e}")
        
        input("\nPress Enter to continue...")

    def discount_report(self):
        """Option 3: Run discount effectiveness report"""
        print("🎁 DISCOUNT EFFECTIVENESS REPORT")
        print("=" * 35)
        try:
            print("📊 Running discount analysis...")
            exec(open('discount_report.py').read())
            print("\n✅ Discount report completed!")
        except Exception as e:
            print(f"❌ Error running discount report: {e}")
        
        input("\nPress Enter to continue...")

    def retail_dashboard(self):
        """Option 4: Complete retail analytics dashboard"""
        print("📊 RETAIL ANALYTICS DASHBOARD")
        print("=" * 32)
        try:
            print("📈 Generating comprehensive dashboard...")
            exec(open('final_summary.py').read())
            print("\n✅ Retail dashboard completed!")
        except Exception as e:
            print(f"❌ Error running retail dashboard: {e}")
        
        input("\nPress Enter to continue...")

    def final_summary(self):
        """Option 5: Final summary report"""
        print("📋 FINAL SUMMARY REPORT")
        print("=" * 25)
        try:
            print("📊 Generating final summary...")
            exec(open('visualization_insights.py').read())
            print("\n✅ Final summary completed!")
        except Exception as e:
            print(f"❌ Error running final summary: {e}")
        
        input("\nPress Enter to continue...")

    def export_to_csv(self):
        """Option 6: Export all data to CSV"""
        print("💾 EXPORTING DATA TO CSV")
        print("=" * 25)
        
        try:
            with duckdb.connect('sales_timeseries.db', read_only=True) as con:
                print("📊 Checking database...")
                
                # Get table info
                table_info = con.execute("SELECT COUNT(*) as total_rows FROM sales_data").fetchone()
                print(f"📈 Total records to export: {table_info[0]:,}")
                
                # Export options
                print("\n📋 Export Options:")
                print("   1. Complete dataset (all columns)")
                print("   2. Sales summary by country")
                print("   3. Hourly sales analysis")
                print("   4. Discount analysis")
                print("   5. Age group analysis")
                print("   6. Product performance")
                print("   7. All of the above (separate files)")
                
                choice = input("\nSelect export option (1-7): ").strip()
                
                if choice == '1':
                    self.export_complete_dataset(con)
                elif choice == '2':
                    self.export_sales_summary(con)
                elif choice == '3':
                    self.export_hourly_analysis(con)
                elif choice == '4':
                    self.export_discount_analysis(con)
                elif choice == '5':
                    self.export_age_analysis(con)
                elif choice == '6':
                    self.export_product_analysis(con)
                elif choice == '7':
                    self.export_all_analyses(con)
                else:
                    print("❌ Invalid option selected.")
                    
        except Exception as e:
            print(f"❌ Error during export: {e}")
        
        input("\nPress Enter to continue...")

    def export_complete_dataset(self, con):
        """Export complete dataset"""
        print("\n📁 Exporting complete dataset...")
        
        # Check size and warn if large
        count = con.execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]
        if count > 1000000:
            print(f"⚠️  Large dataset ({count:,} rows). This may take a while.")
            confirm = input("Continue? (y/N): ").lower()
            if confirm != 'y':
                return
        
        df = con.execute("SELECT * FROM sales_data").df()
        filename = f"complete_sales_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Complete dataset exported to: {filename}")
        print(f"📊 Exported {len(df):,} rows with {len(df.columns)} columns")

    def export_sales_summary(self, con):
        """Export sales summary by country"""
        print("\n📁 Exporting sales summary by country...")
        
        df = con.execute("""
            SELECT 
                country,
                COUNT(*) as total_transactions,
                SUM(total_amount_per_product_sgd) as total_revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction_value,
                COUNT(DISTINCT customer_number) as unique_customers,
                COUNT(DISTINCT product_name) as unique_products
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY country
            ORDER BY total_revenue DESC
        """).df()
        
        filename = f"sales_summary_by_country_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Sales summary exported to: {filename}")

    def export_hourly_analysis(self, con):
        """Export hourly sales analysis"""
        print("\n📁 Exporting hourly sales analysis...")
        
        df = con.execute("""
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
        
        filename = f"hourly_sales_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Hourly analysis exported to: {filename}")

    def export_discount_analysis(self, con):
        """Export discount analysis"""
        print("\n📁 Exporting discount analysis...")
        
        # Discount periods summary
        df1 = con.execute("""
            SELECT 
                discount_period,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction,
                COUNT(DISTINCT customer_number) as customers
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale' AND discount_applied = true
            GROUP BY discount_period
            ORDER BY revenue DESC
        """).df()
        
        filename1 = f"discount_periods_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df1.to_csv(filename1, index=False)
        
        # Regular vs Discount comparison
        df2 = con.execute("""
            SELECT 
                CASE WHEN discount_applied THEN 'Discounted' ELSE 'Regular Price' END as price_type,
                COUNT(*) as transactions,
                SUM(total_amount_per_product_sgd) as revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction,
                COUNT(DISTINCT customer_number) as customers
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY discount_applied
            ORDER BY revenue DESC
        """).df()
        
        filename2 = f"regular_vs_discount_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df2.to_csv(filename2, index=False)
        
        print("✅ Discount analysis exported to:")
        print(f"   📊 {filename1}")
        print(f"   📊 {filename2}")

    def export_age_analysis(self, con):
        """Export age group analysis"""
        print("\n📁 Exporting age group analysis...")
        
        df = con.execute("""
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
            WHERE age IS NOT NULL AND transaction_desc = 'Product Sale'
            GROUP BY country, age_group
            ORDER BY country, age_group
        """).df()
        
        filename = f"age_group_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Age group analysis exported to: {filename}")

    def export_product_analysis(self, con):
        """Export product performance analysis"""
        print("\n📁 Exporting product performance analysis...")
        
        df = con.execute("""
            SELECT 
                product_name,
                COUNT(*) as total_transactions,
                SUM(total_amount_per_product_sgd) as total_revenue,
                AVG(total_amount_per_product_sgd) as avg_transaction_value,
                COUNT(DISTINCT customer_number) as unique_customers,
                COUNT(DISTINCT country) as countries_sold_in,
                SUM(CASE WHEN discount_applied THEN 1 ELSE 0 END) as discounted_sales,
                AVG(CASE WHEN discount_applied THEN total_amount_per_product_sgd ELSE NULL END) as avg_discounted_price
            FROM sales_data 
            WHERE transaction_desc = 'Product Sale'
            GROUP BY product_name
            ORDER BY total_revenue DESC
        """).df()
        
        filename = f"product_performance_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Product analysis exported to: {filename}")

    def export_all_analyses(self, con):
        """Export all analyses to separate files"""
        print("\n📁 Exporting all analyses...")
        
        self.export_sales_summary(con)
        self.export_hourly_analysis(con)
        self.export_discount_analysis(con)
        self.export_age_analysis(con)
        self.export_product_analysis(con)
        
        print("\n🎉 All analyses exported successfully!")

    def database_info(self):
        """Option 7: Show database schema and info"""
        print("🗃️ DATABASE SCHEMA & INFO")
        print("=" * 27)
        
        try:
            with duckdb.connect('sales_timeseries.db', read_only=True) as con:
                # Schema
                print("📋 Database Schema:")
                schema = con.execute("DESCRIBE sales_data").fetchall()
                for row in schema:
                    print(f"   📊 {row[0]}: {row[1]}")
                
                # Basic stats
                print("\n📈 Database Statistics:")
                stats = con.execute("""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT customer_number) as unique_customers,
                        COUNT(DISTINCT product_name) as unique_products,
                        COUNT(DISTINCT country) as unique_countries,
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date,
                        SUM(total_amount_per_product_sgd) as total_revenue
                    FROM sales_data
                """).fetchone()
                
                print(f"   📊 Total Records: {stats[0]:,}")
                print(f"   👥 Unique Customers: {stats[1]:,}")
                print(f"   🛍️ Unique Products: {stats[2]:,}")
                print(f"   🌏 Countries: {stats[3]:,}")
                print(f"   📅 Date Range: {stats[4]} to {stats[5]}")
                print(f"   💰 Total Revenue: SGD ${stats[6]:,.2f}")
                
                # Transaction types
                print("\n🔄 Transaction Types:")
                trans_types = con.execute("""
                    SELECT transaction_desc, COUNT(*) as count, 
                           ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sales_data), 2) as percentage
                    FROM sales_data 
                    GROUP BY transaction_desc 
                    ORDER BY count DESC
                """).fetchall()
                
                for trans_type, count, percentage in trans_types:
                    print(f"   📈 {trans_type}: {count:,} ({percentage}%)")
                    
        except Exception as e:
            print(f"❌ Error accessing database: {e}")
        
        input("\nPress Enter to continue...")

    def quick_insights(self):
        """Option 8: Quick business insights"""
        print("💡 QUICK BUSINESS INSIGHTS")
        print("=" * 27)
        
        try:
            with duckdb.connect('sales_timeseries.db', read_only=True) as con:
                # Best performing metrics
                print("🏆 TOP PERFORMERS:")
                
                # Best country
                best_country = con.execute("""
                    SELECT country, SUM(total_amount_per_product_sgd) as revenue
                    FROM sales_data WHERE transaction_desc = 'Product Sale'
                    GROUP BY country ORDER BY revenue DESC LIMIT 1
                """).fetchone()
                print(f"   🌏 Best Country: {best_country[0]} (SGD ${best_country[1]:,.2f})")
                
                # Best product
                best_product = con.execute("""
                    SELECT product_name, SUM(total_amount_per_product_sgd) as revenue
                    FROM sales_data WHERE transaction_desc = 'Product Sale'
                    GROUP BY product_name ORDER BY revenue DESC LIMIT 1
                """).fetchone()
                print(f"   🛍️ Best Product: {best_product[0]} (SGD ${best_product[1]:,.2f})")
                
                # Best hour
                best_hour = con.execute("""
                    SELECT hour, SUM(total_amount_per_product_sgd) as revenue
                    FROM sales_data WHERE transaction_desc = 'Product Sale'
                    GROUP BY hour ORDER BY revenue DESC LIMIT 1
                """).fetchone()
                print(f"   🕐 Best Hour: {int(best_hour[0])}:00 (SGD ${best_hour[1]:,.2f})")
                
                # Best discount period
                best_discount = con.execute("""
                    SELECT discount_period, SUM(total_amount_per_product_sgd) as revenue
                    FROM sales_data WHERE transaction_desc = 'Product Sale' AND discount_applied = true
                    GROUP BY discount_period ORDER BY revenue DESC LIMIT 1
                """).fetchone()
                if best_discount:
                    print(f"   🎁 Best Discount Period: {best_discount[0]} (SGD ${best_discount[1]:,.2f})")
                
                print("\n📊 KEY METRICS:")
                metrics = con.execute("""
                    SELECT 
                        AVG(total_amount_per_product_sgd) as avg_transaction,
                        COUNT(DISTINCT DATE(date)) as days_of_operation,
                        SUM(total_amount_per_product_sgd) / COUNT(DISTINCT DATE(date)) as avg_daily_revenue
                    FROM sales_data WHERE transaction_desc = 'Product Sale'
                """).fetchone()
                
                print(f"   💰 Average Transaction: SGD ${metrics[0]:.2f}")
                print(f"   📅 Days of Operation: {metrics[1]:,}")
                print(f"   📈 Average Daily Revenue: SGD ${metrics[2]:,.2f}")
                
        except Exception as e:
            print(f"❌ Error generating insights: {e}")
        
        input("\nPress Enter to continue...")

    def generate_all_visualizations(self):
        """Option 9: Generate all visualizations"""
        print("📊 GENERATING ALL VISUALIZATIONS")
        print("=" * 35)
        
        try:
            print("📈 Generating retail dashboard...")
            exec(open('final_summary.py').read())
            
            print("\n📈 Generating hourly analysis...")
            exec(open('hourly_discount_analysis.py').read())
            
            print("\n✅ All visualizations generated successfully!")
            print("📁 Files created:")
            print("   📊 retail_analytics_dashboard.png")
            print("   📊 hourly_sales_analysis.png")
            
        except Exception as e:
            print(f"❌ Error generating visualizations: {e}")
        
        input("\nPress Enter to continue...")

    def main_menu(self):
        """Main menu loop"""
        while True:
            self.display_menu()
            choice = input("Select an option (0-9): ").strip()
            if choice == '1':
                self.recreate_database()
            elif choice == '2':
                self.hourly_analysis()
            elif choice == '3':
                self.discount_report()
            elif choice == '4':
                self.retail_dashboard()
            elif choice == '5':
                self.final_summary()
            elif choice == '6':
                self.export_to_csv()
            elif choice == '7':
                self.database_info()
            elif choice == '8':
                self.quick_insights()
            elif choice == '9':
                self.generate_all_visualizations()
            elif choice == '0':
                self.clear_screen()
                print("👋 Thank you for using Retail Analytics System!")
                print("🎉 Goodbye!")
                sys.exit(0)
            else:
                print("❌ Invalid option. Please select 0-9.")
                input("\nPress Enter to continue...")

if __name__ == "__main__":
    RetailMenu().main_menu()
