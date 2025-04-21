import pandas as pd
import sqlite3
from datetime import datetime
import uuid
import numpy as np

def load_customer_data(csv_file):
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
    conn = sqlite3.connect('data/farm_customers.db')
    
    # Load data into customers table
    customers_df.to_sql('customers', conn, if_exists='append', index=False)
    
    # Create orders from purchase data
    orders_data = []
    order_items_data = []
    
    for _, row in df.iterrows():
        order_id = f'ORD_{uuid.uuid4().hex[:8]}'
        order_date = datetime.strptime(row['Purchase Date'], '%d-%m-%Y').strftime('%Y-%m-%d %H:%M:%S')
        
        # Create order
        orders_data.append((
            order_id,
            row['customer_id'],
            order_date,
            float(row['Purchase Quantity']),  # Using quantity as total amount for simplicity
            'Completed'
        ))
        
        # Create order item
        order_items_data.append((
            order_id,
            f'PROD_{row["Purchase Item"].lower().replace(" ", "_")}',
            int(row['Purchase Quantity']),
            10.0  # Fixed price for simplicity
        ))
    
    # Insert orders
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
        VALUES (?, ?, ?, ?, ?)
    ''', orders_data)
    
    # Insert order items
    cursor.executemany('''
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES (?, ?, ?, ?)
    ''', order_items_data)
    
    # Insert products
    products = df['Purchase Item'].unique()
    products_data = [
        (f'PROD_{prod.lower().replace(" ", "_")}', prod, f'Organic {prod}', 10.0, 'Organic')
        for prod in products
    ]
    
    cursor.executemany('''
        INSERT INTO products (product_id, name, description, price, category)
        VALUES (?, ?, ?, ?, ?)
    ''', products_data)
    
    # Commit and close
    conn.commit()
    conn.close()
    print(f"Loaded {len(df)} customer records into the database")
    print("Database file created at: data/farm_customers.db")

def generate_sample_orders():
    conn = sqlite3.connect('data/farm_customers.db')
    cursor = conn.cursor()
    
    # Get all customer IDs
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    # Sample products
    products = [
        ('P001', 'Organic Tomatoes', 'Fresh organic tomatoes', 4.99, 'Vegetables'),
        ('P002', 'Organic Lettuce', 'Fresh organic lettuce', 3.99, 'Vegetables'),
        ('P003', 'Organic Carrots', 'Fresh organic carrots', 2.99, 'Vegetables'),
        ('P004', 'Organic Apples', 'Fresh organic apples', 5.99, 'Fruits'),
        ('P005', 'Organic Honey', 'Pure organic honey', 8.99, 'Honey')
    ]
    
    # Insert sample products
    cursor.executemany('''
        INSERT INTO products (product_id, name, description, price, category)
        VALUES (?, ?, ?, ?, ?)
    ''', products)
    
    # Generate sample orders
    for customer_id in customer_ids[:10]:  # Generate orders for first 10 customers
        order_id = f"ORD{datetime.now().strftime('%Y%m%d%H%M%S')}{customer_id}"
        order_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        total_amount = 0
        
        # Insert order
        cursor.execute('''
            INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (order_id, customer_id, order_date, 0, 'Completed'))
        
        # Add 1-3 items to each order
        num_items = np.random.randint(1, 4)
        for _ in range(num_items):
            product = products[np.random.randint(0, len(products))]
            quantity = np.random.randint(1, 5)
            unit_price = product[3]
            total_amount += quantity * unit_price
            
            cursor.execute('''
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (?, ?, ?, ?)
            ''', (order_id, product[0], quantity, unit_price))
        
        # Update order total
        cursor.execute('''
            UPDATE orders SET total_amount = ? WHERE order_id = ?
        ''', (total_amount, order_id))
    
    conn.commit()
    conn.close()
    print("Generated sample orders and products")

if __name__ == "__main__":
    # Load customer data from CSV
    load_customer_data('final_synthetic_organic_farm_customers.csv')
    
    # Generate sample orders and products
    generate_sample_orders() 