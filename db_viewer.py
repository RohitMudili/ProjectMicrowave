import sqlite3
import pandas as pd
import os
import json
from datetime import datetime

def export_database_to_json():
    """
    Export the entire database to JSON files for easy viewing in DB Browser for SQLite.
    This creates separate JSON files for each table in the database.
    """
    # Database path
    db_path = 'data/farm_customers.db'
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    
    # Get all table names
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # Create output directory if it doesn't exist
    output_dir = 'db_exports'
    os.makedirs(output_dir, exist_ok=True)
    
    # Export each table to a separate JSON file
    for table in tables:
        table_name = table[0]
        
        # Skip sqlite_sequence table
        if table_name == 'sqlite_sequence':
            continue
        
        # Get table data
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        
        # Convert datetime objects to strings
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Try to convert to datetime and back to string
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
        
        # Export to JSON
        output_file = os.path.join(output_dir, f"{table_name}.json")
        df.to_json(output_file, orient='records', indent=2)
        print(f"Exported {table_name} to {output_file}")
    
    # Export database schema
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
    schemas = cursor.fetchall()
    
    schema_file = os.path.join(output_dir, "database_schema.txt")
    with open(schema_file, 'w') as f:
        f.write("Database Schema:\n\n")
        for schema in schemas:
            f.write(f"{schema[0]}\n\n")
    
    print(f"Exported database schema to {schema_file}")
    
    # Close connection
    conn.close()
    
    print("\nTo view the database in DB Browser for SQLite:")
    print(f"1. Open DB Browser for SQLite")
    print(f"2. Click 'Open Database'")
    print(f"3. Navigate to {os.path.abspath(db_path)}")
    print(f"4. Select the file and click 'Open'")
    print("\nAlternatively, you can view the exported JSON files in the 'db_exports' directory.")

def print_table_info():
    """
    Print information about each table in the database.
    """
    # Database path
    db_path = 'data/farm_customers.db'
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("Database Tables Information:")
    print("===========================")
    
    for table in tables:
        table_name = table[0]
        
        # Skip sqlite_sequence table
        if table_name == 'sqlite_sequence':
            continue
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get column information
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print(f"\nTable: {table_name}")
        print(f"Row count: {row_count}")
        print("Columns:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
    
    # Close connection
    conn.close()

if __name__ == "__main__":
    print("Database Viewer for SQLite")
    print("=========================")
    print("\nThis script helps you view the database in DB Browser for SQLite.")
    print("\nOptions:")
    print("1. Export database to JSON files")
    print("2. Print table information")
    print("3. Exit")
    
    choice = input("\nEnter your choice (1-3): ")
    
    if choice == '1':
        export_database_to_json()
    elif choice == '2':
        print_table_info()
    elif choice == '3':
        print("Exiting...")
    else:
        print("Invalid choice. Exiting...") 