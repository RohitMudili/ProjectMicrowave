import sqlite3
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path: str = 'data/farm_customers.db'):
        self.db_path = db_path
        self.start_date = None
        self.end_date = None

    def set_date_filter(self, start_date: datetime = None, end_date: datetime = None):
        self.start_date = start_date
        self.end_date = end_date

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def execute_query_df(self, query: str, params: tuple = None) -> pd.DataFrame:
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_customer_orders(self, customer_id: str) -> pd.DataFrame:
        query = """
        SELECT o.order_id, o.order_date, o.total_amount, o.status,
               oi.product_id, p.name as product_name, oi.quantity, oi.unit_price
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE o.customer_id = ?
        """
        params = [customer_id]
        
        if self.start_date:
            query += " AND o.order_date >= ?"
            params.append(self.start_date.strftime('%Y-%m-%d'))
        if self.end_date:
            query += " AND o.order_date <= ?"
            params.append(self.end_date.strftime('%Y-%m-%d'))
            
        query += " ORDER BY o.order_date DESC"
        return self.execute_query_df(query, tuple(params))

    def get_top_customers(self, limit: int = 10) -> pd.DataFrame:
        query = """
        SELECT c.customer_id, c.first_name, c.last_name,
               COUNT(o.order_id) as total_orders,
               SUM(o.total_amount) as total_spent,
               MAX(o.order_date) as last_order_date
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        """
        
        params = []
        if self.start_date or self.end_date:
            query += " WHERE 1=1"
            if self.start_date:
                query += " AND o.order_date >= ?"
                params.append(self.start_date.strftime('%Y-%m-%d'))
            if self.end_date:
                query += " AND o.order_date <= ?"
                params.append(self.end_date.strftime('%Y-%m-%d'))
        
        query += """
        GROUP BY c.customer_id
        ORDER BY total_spent DESC
        LIMIT ?
        """
        params.append(limit)
        return self.execute_query_df(query, tuple(params))

    def get_product_sales(self) -> pd.DataFrame:
        query = """
        SELECT p.product_id, p.name, p.category,
               COUNT(DISTINCT o.order_id) as times_ordered,
               SUM(oi.quantity) as total_quantity,
               SUM(oi.quantity * oi.unit_price) as total_revenue
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        """
        
        params = []
        if self.start_date or self.end_date:
            query += " WHERE 1=1"
            if self.start_date:
                query += " AND o.order_date >= ?"
                params.append(self.start_date.strftime('%Y-%m-%d'))
            if self.end_date:
                query += " AND o.order_date <= ?"
                params.append(self.end_date.strftime('%Y-%m-%d'))
        
        query += """
        GROUP BY p.product_id
        ORDER BY total_revenue DESC
        """
        return self.execute_query_df(query, tuple(params) if params else None)

    def get_sales_by_category(self) -> pd.DataFrame:
        query = """
        SELECT p.category,
               COUNT(DISTINCT o.order_id) as total_orders,
               SUM(oi.quantity) as total_quantity,
               SUM(oi.quantity * oi.unit_price) as total_revenue
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        """
        
        params = []
        if self.start_date or self.end_date:
            query += " WHERE 1=1"
            if self.start_date:
                query += " AND o.order_date >= ?"
                params.append(self.start_date.strftime('%Y-%m-%d'))
            if self.end_date:
                query += " AND o.order_date <= ?"
                params.append(self.end_date.strftime('%Y-%m-%d'))
        
        query += """
        GROUP BY p.category
        ORDER BY total_revenue DESC
        """
        return self.execute_query_df(query, tuple(params) if params else None)

    def search_customers(self, search_term: str, search_type: str = None) -> pd.DataFrame:
        """
        Search customers by various fields.
        Args:
            search_term: The term to search for
            search_type: The type of field to search in (name, email, phone, address, city, state)
        """
        if search_type:
            # Search in specific field
            field_map = {
                'Name': '(first_name LIKE ? OR last_name LIKE ?)',
                'Email': 'email LIKE ?',
                'Phone': 'phone LIKE ?',
                'Address': 'address LIKE ?',
                'City': 'city LIKE ?',
                'State': 'state LIKE ?'
            }
            
            if search_type not in field_map:
                raise ValueError(f"Invalid search type. Must be one of: {', '.join(field_map.keys())}")
            
            query = f"""
            SELECT *
            FROM customers
            WHERE {field_map[search_type]}
            """
            
            # Handle name search which needs two parameters
            if search_type == 'Name':
                search_pattern = f"%{search_term}%"
                params = (search_pattern, search_pattern)
            else:
                search_pattern = f"%{search_term}%"
                params = (search_pattern,)
        else:
            # Search across all fields
            query = """
            SELECT *
            FROM customers
            WHERE first_name LIKE ? 
               OR last_name LIKE ? 
               OR email LIKE ? 
               OR phone LIKE ? 
               OR address LIKE ? 
               OR city LIKE ? 
               OR state LIKE ?
            """
            search_pattern = f"%{search_term}%"
            params = (search_pattern,) * 7
        
        return self.execute_query_df(query, params)

    def get_sales_trends(self, days: int = 30) -> pd.DataFrame:
        query = """
        SELECT DATE(o.order_date) as date, SUM(o.total_amount) as revenue
        FROM orders o
        WHERE 1=1
        """
        
        params = []
        if self.start_date:
            query += " AND o.order_date >= ?"
            params.append(self.start_date.strftime('%Y-%m-%d'))
        elif days:
            query += " AND o.order_date >= date('now', ?)"
            params.append(f'-{days} days')
            
        if self.end_date:
            query += " AND o.order_date <= ?"
            params.append(self.end_date.strftime('%Y-%m-%d'))
        
        query += """
        GROUP BY DATE(o.order_date)
        ORDER BY date
        """
        return self.execute_query_df(query, tuple(params) if params else None)

    def get_all_customers(self) -> pd.DataFrame:
        query = """
        SELECT c.*, 
               COUNT(o.order_id) as total_orders,
               SUM(o.total_amount) as total_spent,
               MAX(o.order_date) as last_order_date
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        """
        
        params = []
        if self.start_date or self.end_date:
            query += " WHERE 1=1"
            if self.start_date:
                query += " AND o.order_date >= ?"
                params.append(self.start_date.strftime('%Y-%m-%d'))
            if self.end_date:
                query += " AND o.order_date <= ?"
                params.append(self.end_date.strftime('%Y-%m-%d'))
        
        query += " GROUP BY c.customer_id"
        return self.execute_query_df(query, tuple(params) if params else None)

    def get_all_products(self) -> pd.DataFrame:
        query = """
        SELECT *
        FROM products
        """
        return self.execute_query_df(query)

    def get_all_orders(self) -> pd.DataFrame:
        query = """
        SELECT o.*, oi.product_id, oi.quantity, oi.unit_price
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        """
        
        params = []
        if self.start_date or self.end_date:
            query += " WHERE 1=1"
            if self.start_date:
                query += " AND o.order_date >= ?"
                params.append(self.start_date.strftime('%Y-%m-%d'))
            if self.end_date:
                query += " AND o.order_date <= ?"
                params.append(self.end_date.strftime('%Y-%m-%d'))
        
        return self.execute_query_df(query, tuple(params) if params else None)

if __name__ == "__main__":
    # Example usage
    db = DatabaseManager()
    
    # Get top 5 customers
    print("\nTop 5 Customers:")
    print(db.get_top_customers(5))
    
    # Get product sales
    print("\nProduct Sales:")
    print(db.get_product_sales())
    
    # Get sales by category
    print("\nSales by Category:")
    print(db.get_sales_by_category()) 