import duckdb

SALES_TIMESERIES_DB = 'src/sales_timeseries.db'

def sales_by_customer_income():
    query = '''
        SELECT customer_number, income, SUM(total_amount_per_product_sgd) AS total_sales
        FROM sales_data
        GROUP BY customer_number, income
        ORDER BY income DESC, total_sales DESC
    '''
    with duckdb.connect(SALES_TIMESERIES_DB, read_only=True) as con:
        df = con.execute(query).df()
    print(df)
    return df

if __name__ == "__main__":
    sales_by_customer_income()
