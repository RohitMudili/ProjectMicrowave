import pandas as pd
import sqlite3
from datetime import datetime
import uuid
import numpy as np
import time
import os

def get_db_connection():
    """Create a database connection with proper timeout and isolation level."""
    conn = sqlite3.connect('data/pizza_customers.db', timeout=20)
    conn.execute('PRAGMA journal_mode=WAL')  # Use Write-Ahead Logging
    return conn

def load_customer_data(csv_file):
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Rename columns to match database schema
            df = df.rename(columns={
                'First Name': 'first_name',
                'Last Name': 'last_name',
                'Street Address': 'address',
                'Zip Code': 'zip_code',
                'City': 'city',
                'State': 'state'
            })
            
            # Generate unique customer IDs and email/phone
            df['customer_id'] = [f'CUST_{uuid.uuid4().hex[:8]}' for _ in range(len(df))]
            df['email'] = df.apply(lambda x: f"{x['first_name'].lower()}.{x['last_name'].lower()}@example.com", axis=1)
            df['phone'] = df.apply(lambda x: f"+1-555-{np.random.randint(100,999)}-{np.random.randint(1000,9999)}", axis=1)
            
            # Select and reorder columns to match database schema
            customers_df = df[['customer_id', 'first_name', 'last_name', 'email', 'phone', 
                          'address', 'city', 'state', 'zip_code']]
            
            # Connect to the database
            conn = get_db_connection()
            
            try:
                # Load data into customers table
                customers_df.to_sql('customers', conn, if_exists='append', index=False)
                
                # Create sample products
                products = [
                    ('P001', 'Margherita', 'Classic tomato and mozzarella', 10.99, 'Pizza', 'Medium'),
                    ('P002', 'Pepperoni', 'Classic pepperoni pizza', 12.99, 'Pizza', 'Medium'),
                    ('P003', 'Vegetarian', 'Mixed vegetable pizza', 11.99, 'Pizza', 'Medium'),
                    ('P004', 'Hawaiian', 'Ham and pineapple pizza', 13.99, 'Pizza', 'Medium'),
                    ('P005', 'BBQ Chicken', 'BBQ sauce and chicken pizza', 14.99, 'Pizza', 'Medium'),
                    ('D001', 'Garlic Bread', 'Fresh baked garlic bread', 4.99, 'Side', 'Regular'),
                    ('D002', 'Caesar Salad', 'Fresh Caesar salad', 6.99, 'Side', 'Regular'),
                    ('D003', 'Chicken Wings', 'Spicy chicken wings', 8.99, 'Side', 'Regular'),
                    ('B001', 'Coke', 'Regular Coke', 2.99, 'Beverage', 'Regular'),
                    ('B002', 'Sprite', 'Regular Sprite', 2.99, 'Beverage', 'Regular')
                ]
                
                # Insert products
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT INTO products (product_id, name, description, price, category, size)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', products)
                
                # Create sample toppings
                toppings = [
                    ('T001', 'Extra Cheese', 1.99, 'Cheese'),
                    ('T002', 'Pepperoni', 1.99, 'Meat'),
                    ('T003', 'Mushrooms', 1.49, 'Vegetable'),
                    ('T004', 'Onions', 1.49, 'Vegetable'),
                    ('T005', 'Sausage', 1.99, 'Meat'),
                    ('T006', 'Bell Peppers', 1.49, 'Vegetable'),
                    ('T007', 'Olives', 1.49, 'Vegetable'),
                    ('T008', 'Bacon', 1.99, 'Meat')
                ]
                
                # Insert toppings
                cursor.executemany('''
                    INSERT INTO toppings (topping_id, name, price, category)
                    VALUES (?, ?, ?, ?)
                ''', toppings)
                
                # Create sample orders
                orders_data = []
                order_items_data = []
                
                for _, row in df.iterrows():
                    # Generate 1-3 orders per customer
                    num_orders = np.random.randint(1, 4)
                    for _ in range(num_orders):
                        order_id = f'ORD_{uuid.uuid4().hex[:8]}'
                        order_date = datetime.now() - pd.Timedelta(days=np.random.randint(0, 365))
                        
                        # Random payment method and delivery type
                        payment_methods = ['Credit Card', 'Cash', 'Debit Card']
                        delivery_types = ['Delivery', 'Pickup']
                        
                        # Create order
                        orders_data.append((
                            order_id,
                            row['customer_id'],
                            order_date.strftime('%Y-%m-%d %H:%M:%S'),
                            np.random.uniform(20, 50),  # Random total amount
                            'Completed',
                            np.random.choice(payment_methods),
                            np.random.choice(delivery_types)
                        ))
                        
                        # Add 1-3 items to each order
                        num_items = np.random.randint(1, 4)
                        for _ in range(num_items):
                            product = products[np.random.randint(0, len(products))]
                            quantity = np.random.randint(1, 3)
                            
                            # Random toppings (0-3 toppings per item)
                            num_toppings = np.random.randint(0, 4)
                            selected_toppings = np.random.choice([t[0] for t in toppings], num_toppings, replace=False)
                            toppings_str = ','.join(selected_toppings)
                            
                            order_items_data.append((
                                order_id,
                                product[0],
                                quantity,
                                product[3],
                                toppings_str
                            ))
                
                # Insert orders
                cursor.executemany('''
                    INSERT INTO orders (order_id, customer_id, order_date, total_amount, status, payment_method, delivery_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', orders_data)
                
                # Insert order items
                cursor.executemany('''
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price, toppings)
                    VALUES (?, ?, ?, ?, ?)
                ''', order_items_data)
                
                # Commit and close
                conn.commit()
                print(f"Loaded {len(df)} customer records into the database")
                print("Database file created at: data/pizza_customers.db")
                return  # Success, exit the function
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    if attempt < max_retries - 1:
                        print(f"Database is locked. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise Exception("Failed to access database after multiple attempts. Please ensure no other process is using the database.")
                else:
                    raise
            finally:
                conn.close()
                
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Error occurred: {str(e)}. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to load data after {max_retries} attempts: {str(e)}")

if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Load customer data from CSV
    load_customer_data('pizza_shop_customers_final.csv') 