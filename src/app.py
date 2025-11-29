from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional, List
import duckdb
import uvicorn
from datetime import datetime

class Customer(BaseModel):
    customer_id: Optional[str]
    age: Optional[int]
    country: Optional[str]

class CustomerSummary(BaseModel):
    customer_id: Optional[str]
    age: Optional[int]
    country: Optional[str]
    total_sales: int
    total_amount: float
    
# Pydantic models
class Sale(BaseModel):
    date: Optional[str]
    transaction_id: Optional[str]
    transaction_desc: Optional[str]
    customer_id: Optional[str]
    age: Optional[int]
    gender: Optional[str]
    receipt_number: Optional[str]
    product_id: Optional[str]
    product_name: Optional[str]
    units_sold: Optional[int]
    unit_price_sgd: Optional[float]
    total_amount_per_product_sgd: Optional[float]
    receipt_total_sgd: Optional[float]
    country_id: Optional[str]
    country: str
    city: Optional[str]
    income: Optional[float]

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

class MonthlySalesTrend(BaseModel):
    month: str  # Changed to string to handle DATE_TRUNC result
    revenue: float
    customers: int
    prev_revenue: Optional[float]
    prev_customers: Optional[int]
    revenue_change_pct: Optional[float]
    customer_change_pct: Optional[float]

class CustomerDemographics(BaseModel):
    age_group: str
    gender: Optional[str]
    customer_count: int
    avg_income: Optional[float]
    total_spent: float
    avg_spent_per_customer: float

class HourlySalesDistribution(BaseModel):
    hour_of_day: int
    transaction_count: int
    total_revenue: float
    unique_customers: int

class GeographicSalesDistribution(BaseModel):
    country: str
    transactions: int
    customers: int
    total_revenue: float
    revenue_rank: int

class ValueTransaction(BaseModel):
    value_segment: str
    transaction_count: int
    unique_customers: int
    avg_customer_age: Optional[float]
    avg_transaction_value: float
    total_revenue: float
    relative_to_average: float

class IncomeLevelAnalysis(BaseModel):
    income_segment: str
    customer_count: int
    transaction_count: int
    total_revenue: float
    avg_transaction_value: float
    avg_spent_per_customer: float

# Database connection helper
def get_db_connection():
    return duckdb.connect('src/sales_timeseries.db', read_only=True)

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
        customer_id: Optional[str] = Query(None, description="Filter by customer ID", examples=["C001"]),
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
            customer_id: Filter by specific customer ID
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
                if customer_id:
                    where_conditions.append(f"customer_id = '{customer_id}'")
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
                    SELECT 
                        date, transaction_id, transaction_desc, customer_id, age, gender,
                        receipt_number, product_id, product_name, units_sold, 
                        unit_price_sgd, total_amount_per_product_sgd, receipt_total_sgd,
                        country_id, country, city, income
                    FROM sales_data{where_clause}
                    ORDER BY date DESC
                    LIMIT {page_size} OFFSET {offset}
                """
                
                results = con.execute(data_query).fetchall()
                
                # Convert to list of Sale objects
                sales = []
                for row in results:
                    sale = Sale(
                        date=str(row[0]) if row[0] is not None else None,
                        transaction_id=str(row[1]) if row[1] is not None else None,
                        transaction_desc=str(row[2]) if row[2] is not None else None,
                        customer_id=str(row[3]) if row[3] is not None else None,
                        age=int(row[4]) if row[4] is not None else None,
                        gender=str(row[5]) if row[5] is not None else None,
                        receipt_number=str(row[6]) if row[6] is not None else None,
                        product_id=str(row[7]) if row[7] is not None else None,
                        product_name=str(row[8]) if row[8] is not None else None,
                        units_sold=int(row[9]) if row[9] is not None else None,
                        unit_price_sgd=float(row[10]) if row[10] is not None else None,
                        total_amount_per_product_sgd=float(row[11]) if row[11] is not None else None,
                        receipt_total_sgd=float(row[12]) if row[12] is not None else None,
                        country_id=str(row[13]) if row[13] is not None else None,
                        country=str(row[14]) if row[14] is not None else "Unknown",
                        city=str(row[15]) if row[15] is not None else None,
                        income=float(row[16]) if row[16] is not None else None
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
                        customer_id,
                        age,
                        country
                    FROM sales_data 
                    ORDER BY customer_id
                """
                results = con.execute(query).fetchall()
                customers = []
                for row in results:
                    customer = Customer(
                        customer_id=str(row[0]) if row[0] is not None else None,
                        age=int(row[1]) if row[1] is not None else None,
                        country=str(row[2]) if row[2] is not None else None
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
                        customer_id,
                        age,
                        country,
                        COUNT(*) as total_sales,
                        SUM(total_amount_per_product_sgd) as total_amount
                    FROM sales_data 
                    GROUP BY customer_id, age, country
                    ORDER BY customer_id
                """
                results = con.execute(query).fetchall()
                customers = []
                for row in results:
                    customer = CustomerSummary(
                        customer_id=str(row[0]) if row[0] is not None else None,
                        age=int(row[1]) if row[1] is not None else None,
                        country=str(row[2]) if row[2] is not None else None,
                        total_sales=int(row[3]) if row[3] is not None else 0,
                        total_amount=float(row[4]) if row[4] is not None else 0.0
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
                        COUNT(DISTINCT customer_id) as unique_customers,
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
                top_products_results = con.execute(top_products_query).fetchall()
                
                top_products = []
                for row in top_products_results:
                    top_products.append({
                        "product_id": row[0],
                        "product_name": row[1],
                        "total_units": int(row[2]),
                        "total_revenue": float(row[3]),
                        "avg_price": float(row[4])
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

    @app.get("/analytics/top-products/", response_model=List[ProductSales], tags=["Analytics"])
    def get_top_products():
        """Top Products by Revenue - Shows the best-selling products with sales metrics"""
        try:
            with get_db_connection() as con:
                query = """
                    SELECT 
                        product_id,
                        product_name,
                        COUNT(*) AS transaction_count,
                        SUM(units_sold) AS total_units_sold,
                        SUM(total_amount_per_product_sgd) AS total_revenue,
                        AVG(unit_price_sgd) AS avg_price
                    FROM sales_data
                    GROUP BY product_id, product_name
                    ORDER BY total_revenue DESC
                    LIMIT 20
                """
                results = con.execute(query).fetchall()
                
                products = []
                for row in results:
                    product = ProductSales(
                        product_id=row[0],
                        product_name=row[1],
                        total_units_sold=int(row[3]),
                        total_revenue=float(row[4]),
                        avg_price=float(row[5])
                    )
                    products.append(product)
                
                return products
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/analytics/monthly-trends/", response_model=List[MonthlySalesTrend], tags=["Analytics"])
    def get_monthly_sales_trends():
        """Monthly Sales Trends - Shows revenue and customer trends by month with growth percentages"""
        try:
            with get_db_connection() as con:
                query = """
                    WITH monthly_sales AS (
                        SELECT 
                            DATE_TRUNC('month', date) AS month,
                            SUM(total_amount_per_product_sgd) AS revenue,
                            COUNT(DISTINCT customer_id) AS customers
                        FROM sales_data
                        GROUP BY DATE_TRUNC('month', date)
                        ORDER BY month
                    ),
                    with_prev AS (
                        SELECT 
                            month, 
                            revenue,
                            customers,
                            LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
                            LAG(customers) OVER (ORDER BY month) AS prev_customers
                        FROM monthly_sales
                    )
                    SELECT 
                        month,
                        revenue,
                        customers,
                        prev_revenue,
                        prev_customers,
                        CASE 
                            WHEN prev_revenue IS NULL THEN NULL
                            WHEN prev_revenue = 0 THEN 
                                CASE 
                                    WHEN revenue = 0 THEN 0
                                    ELSE 100 -- from zero to something is 100% growth
                                END
                            ELSE ((revenue - prev_revenue) / prev_revenue) * 100 
                        END AS revenue_change_pct,
                        CASE
                            WHEN prev_customers IS NULL THEN NULL
                            WHEN prev_customers = 0 THEN 
                                CASE
                                    WHEN customers = 0 THEN 0
                                    ELSE 100
                                END
                            ELSE ((customers - prev_customers) / prev_customers) * 100
                        END AS customer_change_pct
                    FROM with_prev
                    ORDER BY month
                """
                results = con.execute(query).fetchall()
                
                trends = []
                for row in results:
                    trends.append(MonthlySalesTrend(
                        month=str(row[0]),
                        revenue=float(row[1]),
                        customers=int(row[2]),
                        prev_revenue=float(row[3]) if row[3] is not None else None,
                        prev_customers=int(row[4]) if row[4] is not None else None,
                        revenue_change_pct=float(row[5]) if row[5] is not None else None,
                        customer_change_pct=float(row[6]) if row[6] is not None else None
                    ))
                
                return trends
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/analytics/customer-demographics/", response_model=List[CustomerDemographics], tags=["Analytics"])
    def get_customer_demographics():
        """Customer Demographics Analysis - Segments customers by age groups and gender with spending patterns"""
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
                        END AS age_group,
                        gender,
                        COUNT(DISTINCT customer_id) AS customer_count,
                        AVG(income) AS avg_income,
                        SUM(total_amount_per_product_sgd) AS total_spent,
                        SUM(total_amount_per_product_sgd) / COUNT(DISTINCT customer_id) AS avg_spent_per_customer
                    FROM sales_data
                    GROUP BY age_group, gender
                    ORDER BY age_group, gender
                """
                results = con.execute(query).fetchall()
                
                demographics = []
                for row in results:
                    demographics.append(CustomerDemographics(
                        age_group=str(row[0]),
                        gender=str(row[1]) if row[1] is not None else None,
                        customer_count=int(row[2]) if row[2] is not None else 0,
                        avg_income=float(row[3]) if row[3] is not None else None,
                        total_spent=float(row[4]) if row[4] is not None else 0.0,
                        avg_spent_per_customer=float(row[5]) if row[5] is not None else 0.0
                    ))
                
                return demographics
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/analytics/hourly-distribution/", response_model=List[HourlySalesDistribution], tags=["Analytics"])
    def get_hourly_sales_distribution():
        """Hourly Sales Distribution - Analyzes sales patterns by hour of day"""
        try:
            with get_db_connection() as con:
                query = """
                    SELECT 
                        EXTRACT(hour FROM date) AS hour_of_day,
                        COUNT(*) AS transaction_count,
                        SUM(total_amount_per_product_sgd) AS total_revenue,
                        COUNT(DISTINCT customer_id) AS unique_customers
                    FROM sales_data
                    GROUP BY hour_of_day
                    ORDER BY hour_of_day
                """
                results = con.execute(query).fetchall()
                
                hourly_data = []
                for row in results:
                    hourly_data.append(HourlySalesDistribution(
                        hour_of_day=int(row[0]),
                        transaction_count=int(row[1]),
                        total_revenue=float(row[2]),
                        unique_customers=int(row[3])
                    ))
                
                return hourly_data
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/analytics/geographic-distribution/", response_model=List[GeographicSalesDistribution], tags=["Analytics"])
    def get_geographic_sales_distribution():
        """Geographic Sales Distribution - Shows sales by country and city with rankings"""
        try:
            with get_db_connection() as con:
                query = """
                    WITH country_sales AS (
                        SELECT 
                            country,
                            COUNT(*) AS transactions,
                            COUNT(DISTINCT customer_id) AS customers,
                            SUM(total_amount_per_product_sgd) AS total_revenue
                        FROM sales_data
                        GROUP BY country
                    ),
                    ranked_countries AS (
                        SELECT 
                            country,
                            transactions,
                            customers,
                            total_revenue,
                            RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank
                        FROM country_sales
                    )
                    SELECT
                        country,
                        transactions,
                        customers,
                        total_revenue,
                        revenue_rank
                    FROM ranked_countries
                    ORDER BY revenue_rank
                    LIMIT 15
                """
                results = con.execute(query).fetchall()
                
                geographic_data = []
                for row in results:
                    geographic_data.append(GeographicSalesDistribution(
                        country=row[0],
                        transactions=int(row[1]),
                        customers=int(row[2]),
                        total_revenue=float(row[3]),
                        revenue_rank=int(row[4])
                    ))
                
                return geographic_data
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/analytics/value-transactions/", response_model=List[ValueTransaction], tags=["Analytics"])
    def get_value_transactions():
        """High-Value vs Low-Value Transactions - Compares highest and lowest value transactions by various dimensions"""
        try:
            with get_db_connection() as con:
                query = """
                    WITH transaction_values AS (
                        SELECT
                            date,
                            transaction_id,
                            customer_id,
                            age,
                            gender,
                            country,
                            city,
                            total_amount_per_product_sgd,
                            NTILE(10) OVER (ORDER BY total_amount_per_product_sgd) AS value_decile
                        FROM sales_data
                    )
                    SELECT
                        CASE 
                            WHEN value_decile = 1 THEN 'Bottom 10%'
                            WHEN value_decile = 10 THEN 'Top 10%'
                        END AS value_segment,
                        COUNT(*) AS transaction_count,
                        COUNT(DISTINCT customer_id) AS unique_customers,
                        AVG(age) AS avg_customer_age,
                        AVG(total_amount_per_product_sgd) AS avg_transaction_value,
                        SUM(total_amount_per_product_sgd) AS total_revenue,
                        (SUM(total_amount_per_product_sgd) / COUNT(*)) / 
                            (SELECT AVG(total_amount_per_product_sgd) FROM sales_data) AS relative_to_average
                    FROM transaction_values
                    WHERE value_decile IN (1, 10)
                    GROUP BY value_segment
                    ORDER BY value_segment
                """
                results = con.execute(query).fetchall()
                
                value_data = []
                for row in results:
                    value_data.append(ValueTransaction(
                        value_segment=str(row[0]),
                        transaction_count=int(row[1]) if row[1] is not None else 0,
                        unique_customers=int(row[2]) if row[2] is not None else 0,
                        avg_customer_age=float(row[3]) if row[3] is not None else None,
                        avg_transaction_value=float(row[4]) if row[4] is not None else 0.0,
                        total_revenue=float(row[5]) if row[5] is not None else 0.0,
                        relative_to_average=float(row[6]) if row[6] is not None else 0.0
                    ))
                
                return value_data
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @app.get("/analytics/income-levels/", response_model=List[IncomeLevelAnalysis], tags=["Analytics"])
    def get_income_level_analysis():
        """Income Level Analysis - Segments customers by income levels with purchasing patterns"""
        try:
            with get_db_connection() as con:
                # First check if income column exists
                schema_query = "DESCRIBE sales_data"
                schema_result = con.execute(schema_query).fetchall()
                column_names = [row[0] for row in schema_result]
                has_income = 'income' in column_names
                
                if has_income:
                    query = """
                        WITH income_segments AS (
                            SELECT
                                CASE
                                    WHEN income < 30000 THEN 'Low Income (< 30k)'
                                    WHEN income BETWEEN 30000 AND 60000 THEN 'Middle Income (30k-60k)'
                                    WHEN income BETWEEN 60001 AND 100000 THEN 'Upper Middle (60k-100k)'
                                    WHEN income > 100000 THEN 'High Income (>100k)'
                                    ELSE 'Unknown'
                                END AS income_segment,
                                customer_id,
                                total_amount_per_product_sgd
                            FROM sales_data
                        )
                        SELECT
                            income_segment,
                            COUNT(DISTINCT customer_id) AS customer_count,
                            COUNT(*) AS transaction_count,
                            SUM(total_amount_per_product_sgd) AS total_revenue,
                            AVG(total_amount_per_product_sgd) AS avg_transaction_value,
                            SUM(total_amount_per_product_sgd) / COUNT(DISTINCT customer_id) AS avg_spent_per_customer
                        FROM income_segments
                        GROUP BY income_segment
                        ORDER BY 
                            CASE 
                                WHEN income_segment = 'Low Income (< 30k)' THEN 1
                                WHEN income_segment = 'Middle Income (30k-60k)' THEN 2
                                WHEN income_segment = 'Upper Middle (60k-100k)' THEN 3
                                WHEN income_segment = 'High Income (>100k)' THEN 4
                                ELSE 5
                            END
                    """
                else:
                    # Fallback to age-based income estimation if income column doesn't exist
                    query = """
                        WITH income_segments AS (
                            SELECT
                                CASE
                                    WHEN age < 25 THEN 'Young Adults (<25)'
                                    WHEN age BETWEEN 25 AND 40 THEN 'Mid Career (25-40)'
                                    WHEN age BETWEEN 41 AND 55 THEN 'Senior Career (41-55)'
                                    WHEN age > 55 THEN 'Pre-Retirement (55+)'
                                    ELSE 'Unknown Age'
                                END AS income_segment,
                                customer_id,
                                total_amount_per_product_sgd
                            FROM sales_data
                        )
                        SELECT
                            income_segment,
                            COUNT(DISTINCT customer_id) AS customer_count,
                            COUNT(*) AS transaction_count,
                            SUM(total_amount_per_product_sgd) AS total_revenue,
                            AVG(total_amount_per_product_sgd) AS avg_transaction_value,
                            SUM(total_amount_per_product_sgd) / COUNT(DISTINCT customer_id) AS avg_spent_per_customer
                        FROM income_segments
                        GROUP BY income_segment
                        ORDER BY 
                            CASE 
                                WHEN income_segment = 'Young Adults (<25)' THEN 1
                                WHEN income_segment = 'Mid Career (25-40)' THEN 2
                                WHEN income_segment = 'Senior Career (41-55)' THEN 3
                                WHEN income_segment = 'Pre-Retirement (55+)' THEN 4
                                ELSE 5
                            END
                    """
                
                results = con.execute(query).fetchall()
                
                income_data = []
                for row in results:
                    income_data.append(IncomeLevelAnalysis(
                        income_segment=row[0],
                        customer_count=int(row[1]),
                        transaction_count=int(row[2]),
                        total_revenue=float(row[3]),
                        avg_transaction_value=float(row[4]),
                        avg_spent_per_customer=float(row[5])
                    ))
                
                return income_data
                
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
                        COUNT(DISTINCT customer_id) as unique_customers,
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

    @app.get("/customers/{customer_id}", tags=["Customers"])
    def get_customer_history(customer_id: str = Path(..., description="Customer ID", examples=["CUST001"])):
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
                    WHERE customer_id = ?
                """
                summary_result = con.execute(summary_query, [customer_id]).fetchone()
                
                if summary_result[0] == 0:
                    raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
                
                # Recent transactions
                recent_query = """
                    SELECT product_name, units_sold, total_amount_per_product_sgd, date
                    FROM sales_data
                    WHERE customer_id = ?
                    ORDER BY date DESC
                    LIMIT 10
                """
                recent_results = con.execute(recent_query, [customer_id]).fetchall()
                
                recent_purchases = []
                for row in recent_results:
                    recent_purchases.append({
                        "product_name": row[0],
                        "units_sold": int(row[1]),
                        "amount": float(row[2]),
                        "date": str(row[3])
                    })
                
                return {
                    "customer_id": customer_id,
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
                    SELECT 
                        date, transaction_id, customer_id, receipt_number,
                        product_id, product_name, units_sold, unit_price_sgd,
                        total_amount_per_product_sgd, receipt_total_sgd
                    FROM sales_data
                    WHERE receipt_number = ?
                    ORDER BY product_name
                """
                results = con.execute(query, [receipt_number]).fetchall()
                
                if not results:
                    raise HTTPException(status_code=404, detail=f"Receipt {receipt_number} not found")
                
                items = []
                for row in results:
                    items.append({
                        "product_id": str(row[4]),
                        "product_name": str(row[5]),
                        "units_sold": int(row[6]) if row[6] is not None else 0,
                        "unit_price_sgd": float(row[7]) if row[7] is not None else 0.0,
                        "total_amount_sgd": float(row[8]) if row[8] is not None else 0.0
                    })
                
                first_row = results[0]
                return {
                    "receipt_number": receipt_number,
                    "customer_id": str(first_row[2]),
                    "date": str(first_row[0]),
                    "receipt_total_sgd": float(first_row[9]) if first_row[9] is not None else 0.0,
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

