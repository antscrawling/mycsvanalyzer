from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional, List
import duckdb
import uvicorn
from datetime import datetime

class Customer(BaseModel):
    customer_number: int
    age: int
    country: str

class CustomerSummary(BaseModel):
    customer_number: int
    age: int
    country: str
    total_sales: int
    total_amount: float
    
# Pydantic models
class Sale(BaseModel):
    date: str
    customer_number: int
    age: int
    receipt_number: int
    product_id: str
    product_name: str
    units_sold: int
    unit_price_sgd: float
    total_amount_per_product_sgd: float
    receipt_total_sgd: float
    day_of_week: int
    month: int
    hour: int
    year: int
    day_of_week_text: str
    month_text: str
    day_of_week_sin: float
    day_of_week_cos: float
    month_sin: float
    month_cos: float
    hour_sin: float
    hour_cos: float
    country: str

class SalesResponse(BaseModel):
    total_records: int
    page: int
    page_size: int
    sales: List[Sale]

class SalesSummary(BaseModel):
    total_records: int
    total_revenue: float
    unique_customers: int
    unique_receipts: int
    date_range_start: str
    date_range_end: str
    top_products: List[dict]

class ProductSales(BaseModel):
    product_id: str
    product_name: str
    total_units_sold: int
    total_revenue: float
    avg_price: float

# Database connection helper
def get_db_connection():
    return duckdb.connect('sales_timeseries.db', read_only=True)

def main():
    app = FastAPI(
        title="Retail Sales API",
        description="API for retail sales time series data",
        version="1.0.0"
    )

    @app.get("/", tags=["Root"])
    def read_root():
        """Redirect to API documentation"""
        return RedirectResponse(url="/docs")

    @app.get("/sales/", response_model=SalesResponse, tags=["Sales"])
    def get_sales(
        page: int = Query(1, ge=1, description="Page number"),
        page_size: int = Query(50, ge=1, le=1000, description="Items per page"),
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format", examples=["2024-01-01"]),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format", examples=["2024-12-31"]),
        product_id: Optional[str] = Query(None, description="Filter by product ID", examples=["P001"]),
        customer_number: Optional[int] = Query(None, description="Filter by customer number", examples=[100001]),
        min_age: Optional[int] = Query(None, ge=18, le=100, description="Minimum customer age", examples=[25]),
        max_age: Optional[int] = Query(None, ge=18, le=100, description="Maximum customer age", examples=[65])
    ):
        """
        Get sales data with pagination and filtering
        
        Args:
            page: Page number (starts from 1)
            page_size: Number of items per page (1-1000)
            start_date: Start date filter in YYYY-MM-DD format
            end_date: End date filter in YYYY-MM-DD format
            product_id: Filter by specific product ID (P001, P002, etc.)
            customer_number: Filter by specific customer number
            min_age: Minimum customer age filter (18-100)
            max_age: Maximum customer age filter (18-100)
            
        Returns:
            Paginated sales data with filtering applied
        """
        # Validate date formats if provided
        if start_date:
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Invalid start_date format. Please use YYYY-MM-DD format (e.g., 2024-01-01)"
                )
        
        if end_date:
            try:
                datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Invalid end_date format. Please use YYYY-MM-DD format (e.g., 2024-12-31)"
                )
        
        try:
            with get_db_connection() as con:
                # Build WHERE clause
                where_conditions = []
                if start_date:
                    where_conditions.append(f"date >= '{start_date}'")
                if end_date:
                    where_conditions.append(f"date <= '{end_date}'")
                if product_id:
                    where_conditions.append(f"product_id = '{product_id}'")
                if customer_number:
                    where_conditions.append(f"customer_number = {customer_number}")
                if min_age:
                    where_conditions.append(f"age >= {min_age}")
                if max_age:
                    where_conditions.append(f"age <= {max_age}")
                
                where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
                
                # Get total count
                count_query = f"SELECT COUNT(*) FROM sales_data{where_clause}"
                total_records = con.execute(count_query).fetchone()[0]
                
                # Get paginated data
                offset = (page - 1) * page_size
                data_query = f"""
                    SELECT * FROM sales_data{where_clause}
                    ORDER BY date DESC
                    LIMIT {page_size} OFFSET {offset}
                """
                
                df = con.execute(data_query).df()
                
                # Convert to list of Sale objects
                sales = []
                for _, row in df.iterrows():
                    sale = Sale(
                        date=str(row['date']),
                        customer_number=int(row['customer_number']),
                        age=int(row['age']),
                        receipt_number=int(row['receipt_number']),
                        product_id=row['product_id'],
                        product_name=row['product_name'],
                        units_sold=int(row['units_sold']),
                        unit_price_sgd=float(row['unit_price_sgd']),
                        total_amount_per_product_sgd=float(row['total_amount_per_product_sgd']),
                        receipt_total_sgd=float(row['receipt_total_sgd']),
                        day_of_week=int(row['day_of_week']),
                        month=int(row['month']),
                        hour=int(row['hour']),
                        year=int(row['year']),
                        day_of_week_text=row['day_of_week_text'],
                        month_text=row['month_text'],
                        day_of_week_sin=float(row['day_of_week_sin']),
                        day_of_week_cos=float(row['day_of_week_cos']),
                        month_sin=float(row['month_sin']),
                        month_cos=float(row['month_cos']),
                        hour_sin=float(row['hour_sin']),
                        hour_cos=float(row['hour_cos']),
                        country=row['country']
                    )
                    sales.append(sale)
                
                return SalesResponse(
                    total_records=total_records,
                    page=page,
                    page_size=page_size,
                    sales=sales
                )
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    @app.get("/customers/", response_model=List[Customer], tags=["Customers"])
    def get_customers():
        """Get list of unique customers"""
        try:
            with get_db_connection() as con:
                # Get unique customers only
                query = """
                    SELECT DISTINCT 
                        customer_number,
                        age,
                        country
                    FROM sales_data 
                    ORDER BY customer_number
                """
                df = con.execute(query).df()
                customers = []
                for _, row in df.iterrows():
                    customer = Customer(
                        customer_number=int(row['customer_number']),
                        age=int(row['age']),
                        country=row['country']
                    )
                    customers.append(customer)
                return customers
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/customers/summary/", response_model=List[CustomerSummary], tags=["Customers"])
    def get_customers_summary():
        """Get customer summary with total sales and amount"""
        try:
            with get_db_connection() as con:
                query = """
                    SELECT 
                        customer_number,
                        age,
                        country,
                        COUNT(*) as total_sales,
                        SUM(total_amount_per_product_sgd) as total_amount
                    FROM sales_data 
                    GROUP BY customer_number, age, country
                    ORDER BY customer_number
                """
                df = con.execute(query).df()
                customers = []
                for _, row in df.iterrows():
                    customer = CustomerSummary(
                        customer_number=int(row['customer_number']),
                        age=int(row['age']),
                        country=row['country'],
                        total_sales=int(row['total_sales']),
                        total_amount=float(row['total_amount'])
                    )
                    customers.append(customer)
                return customers
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/summary/", response_model=SalesSummary, tags=["Analytics"])
    def get_sales_summary():
        """Get overall sales summary statistics"""
        try:
            with get_db_connection() as con:
                # Basic statistics
                summary_query = """
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(total_amount_per_product_sgd) as total_revenue,
                        COUNT(DISTINCT customer_number) as unique_customers,
                        COUNT(DISTINCT receipt_number) as unique_receipts,
                        MIN(date) as date_start,
                        MAX(date) as date_end
                    FROM sales_data
                """
                summary_result = con.execute(summary_query).fetchone()
                
                # Top products
                top_products_query = """
                    SELECT 
                        product_id,
                        product_name,
                        SUM(units_sold) as total_units,
                        SUM(total_amount_per_product_sgd) as total_revenue,
                        AVG(unit_price_sgd) as avg_price
                    FROM sales_data
                    GROUP BY product_id, product_name
                    ORDER BY total_revenue DESC
                    LIMIT 10
                """
                top_products_df = con.execute(top_products_query).df()
                
                top_products = []
                for _, row in top_products_df.iterrows():
                    top_products.append({
                        "product_id": row['product_id'],
                        "product_name": row['product_name'],
                        "total_units": int(row['total_units']),
                        "total_revenue": float(row['total_revenue']),
                        "avg_price": float(row['avg_price'])
                    })
                
                return SalesSummary(
                    total_records=int(summary_result[0]),
                    total_revenue=float(summary_result[1]),
                    unique_customers=int(summary_result[2]),
                    unique_receipts=int(summary_result[3]),
                    date_range_start=str(summary_result[4]),
                    date_range_end=str(summary_result[5]),
                    top_products=top_products
                )
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/products/", response_model=List[ProductSales], tags=["Products"])
    def get_product_performance():
        """Get product performance metrics"""
        try:
            with get_db_connection() as con:
                query = """
                    SELECT 
                        product_id,
                        product_name,
                        SUM(units_sold) as total_units_sold,
                        SUM(total_amount_per_product_sgd) as total_revenue,
                        AVG(unit_price_sgd) as avg_price
                    FROM sales_data
                    GROUP BY product_id, product_name
                    ORDER BY total_revenue DESC
                """
                df = con.execute(query).df()
                
                products = []
                for _, row in df.iterrows():
                    product = ProductSales(
                        product_id=row['product_id'],
                        product_name=row['product_name'],
                        total_units_sold=int(row['total_units_sold']),
                        total_revenue=float(row['total_revenue']),
                        avg_price=float(row['avg_price'])
                    )
                    products.append(product)
                
                return products
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/analytics/age-groups/", tags=["Analytics"])
    def get_age_group_analytics():
        """Get sales analytics by age groups"""
        try:
            with get_db_connection() as con:
                query = """
                    SELECT 
                        CASE 
                            WHEN age BETWEEN 18 AND 25 THEN '18-25'
                            WHEN age BETWEEN 26 AND 35 THEN '26-35'
                            WHEN age BETWEEN 36 AND 45 THEN '36-45'
                            WHEN age BETWEEN 46 AND 55 THEN '46-55'
                            WHEN age BETWEEN 56 AND 65 THEN '56-65'
                            WHEN age > 65 THEN '65+'
                            ELSE 'Unknown'
                        END as age_group,
                        COUNT(*) as transaction_count,
                        SUM(total_amount_per_product_sgd) as total_revenue,
                        AVG(total_amount_per_product_sgd) as avg_transaction_value,
                        COUNT(DISTINCT customer_number) as unique_customers,
                        AVG(age) as avg_age_in_group
                    FROM sales_data
                    WHERE age IS NOT NULL
                    GROUP BY age_group
                    ORDER BY 
                        CASE age_group
                            WHEN '18-25' THEN 1
                            WHEN '26-35' THEN 2
                            WHEN '36-45' THEN 3
                            WHEN '46-55' THEN 4
                            WHEN '56-65' THEN 5
                            WHEN '65+' THEN 6
                            ELSE 7
                        END
                """
                df = con.execute(query).df()
                
                age_groups = []
                for _, row in df.iterrows():
                    age_groups.append({
                        "age_group": row['age_group'],
                        "transaction_count": int(row['transaction_count']),
                        "total_revenue": float(row['total_revenue']),
                        "avg_transaction_value": float(row['avg_transaction_value']),
                        "unique_customers": int(row['unique_customers']),
                        "avg_age_in_group": float(row['avg_age_in_group'])
                    })
                
                return {
                    "age_group_analytics": age_groups,
                    "total_customers_analyzed": int(df['unique_customers'].sum())
                }
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/sales/by-date/{target_date}", tags=["Sales"])
    def get_sales_by_date(target_date: str = Path(..., description="Date in YYYY-MM-DD format", examples=["2024-01-01"])):
        """
        Get all sales for a specific date
        
        Args:
            target_date: Date in YYYY-MM-DD format (e.g., 2024-01-01)
            
        Returns:
            Daily sales summary including transaction count, revenue, customers, and receipts
        """
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Invalid date format. Please use YYYY-MM-DD format (e.g., 2024-01-01)"
            )
            
        try:
            with get_db_connection() as con:
                query = """
                    SELECT 
                        COUNT(*) as transaction_count,
                        SUM(total_amount_per_product_sgd) as daily_revenue,
                        COUNT(DISTINCT customer_number) as unique_customers,
                        COUNT(DISTINCT receipt_number) as unique_receipts,
                        AVG(receipt_total_sgd) as avg_receipt_value
                    FROM sales_data
                    WHERE DATE(date) = ?
                """
                result = con.execute(query, [target_date]).fetchone()
                
                if result[0] == 0:
                    raise HTTPException(status_code=404, detail=f"No sales found for date {target_date}")
                
                return {
                    "date": target_date,
                    "transaction_count": int(result[0]),
                    "daily_revenue": float(result[1]) if result[1] else 0,
                    "unique_customers": int(result[2]),
                    "unique_receipts": int(result[3]),
                    "avg_receipt_value": float(result[4]) if result[4] else 0
                }
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/customers/{customer_number}", tags=["Customers"])
    def get_customer_history(customer_number: int = Path(..., description="Customer number", examples=[100001])):
        """Get purchase history for a specific customer"""
        try:
            with get_db_connection() as con:
                # Customer summary
                summary_query = """
                    SELECT 
                        COUNT(*) as total_transactions,
                        SUM(total_amount_per_product_sgd) as total_spent,
                        COUNT(DISTINCT receipt_number) as total_receipts,
                        MIN(date) as first_purchase,
                        MAX(date) as last_purchase
                    FROM sales_data
                    WHERE customer_number = ?
                """
                summary_result = con.execute(summary_query, [customer_number]).fetchone()
                
                if summary_result[0] == 0:
                    raise HTTPException(status_code=404, detail=f"Customer {customer_number} not found")
                
                # Recent transactions
                recent_query = """
                    SELECT product_name, units_sold, total_amount_per_product_sgd, date
                    FROM sales_data
                    WHERE customer_number = ?
                    ORDER BY date DESC
                    LIMIT 10
                """
                recent_df = con.execute(recent_query, [customer_number]).df()
                
                recent_purchases = []
                for _, row in recent_df.iterrows():
                    recent_purchases.append({
                        "product_name": row['product_name'],
                        "units_sold": int(row['units_sold']),
                        "amount": float(row['total_amount_per_product_sgd']),
                        "date": str(row['date'])
                    })
                
                return {
                    "customer_number": customer_number,
                    "total_transactions": int(summary_result[0]),
                    "total_spent": float(summary_result[1]) if summary_result[1] else 0,
                    "total_receipts": int(summary_result[2]),
                    "first_purchase": str(summary_result[3]),
                    "last_purchase": str(summary_result[4]),
                    "recent_purchases": recent_purchases
                }
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/receipts/{receipt_number}", tags=["Receipts"])
    def get_receipt_details(receipt_number: int = Path(..., description="Receipt number", examples=[200001])):
        """Get all items in a specific receipt"""
        try:
            with get_db_connection() as con:
                query = """
                    SELECT *
                    FROM sales_data
                    WHERE receipt_number = ?
                    ORDER BY product_name
                """
                df = con.execute(query, [receipt_number]).df()
                
                if df.empty:
                    raise HTTPException(status_code=404, detail=f"Receipt {receipt_number} not found")
                
                items = []
                for _, row in df.iterrows():
                    items.append({
                        "product_id": row['product_id'],
                        "product_name": row['product_name'],
                        "units_sold": int(row['units_sold']),
                        "unit_price_sgd": float(row['unit_price_sgd']),
                        "total_amount_sgd": float(row['total_amount_per_product_sgd'])
                    })
                
                return {
                    "receipt_number": receipt_number,
                    "customer_number": int(df.iloc[0]['customer_number']),
                    "date": str(df.iloc[0]['date']),
                    "receipt_total_sgd": float(df.iloc[0]['receipt_total_sgd']),
                    "items": items
                }
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return app

# Create app instance for uvicorn
app = main()

if __name__ == "__main__":
    app = main()
    uvicorn.run(app, host="0.0.0.0", port=8080)
    uvicorn.run()

