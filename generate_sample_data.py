"""
Generate sample sales data for testing the ETL pipeline
Creates CSV files with intentional inconsistencies to demonstrate cleaning
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_store_sales(store_id, start_date, end_date, num_records=200):
    """Generate sales data for a single store"""
    
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    products = [
        'Laptop', 'Desktop', 'Monitor', 'Keyboard', 'Mouse',
        'Headphones', 'Webcam', 'USB Cable', 'HDMI Cable', 'Power Adapter'
    ]
    
    data = []
    
    for _ in range(num_records):
        date = random.choice(dates)
        product = random.choice(products)
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 1500), 2)
        revenue = round(quantity * unit_price, 2)
        
        # Add some missing/invalid data occasionally
        if random.random() < 0.05:
            revenue = None
        if random.random() < 0.03:
            date = None
            
        data.append({
            'store_id': store_id,
            'date': date,
            'product': product,
            'quantity': quantity,
            'revenue': revenue
        })
    
    return pd.DataFrame(data)


def generate_sample_files():
    """Generate sample CSV files with different formats"""
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # 3 months of data
    
    # Store 1 - Standard format
    print("Generating Store 1 data (standard format)...")
    store1 = generate_store_sales('STORE_001', start_date, end_date, 250)
    store1.to_csv('store1_sales.csv', index=False)
    
    # Store 2 - Different column names
    print("Generating Store 2 data (alternative column names)...")
    store2 = generate_store_sales('STORE_002', start_date, end_date, 220)
    store2.rename(columns={
        'date': 'sale_date',
        'revenue': 'amount',
        'quantity': 'qty'
    }, inplace=True)
    store2.to_csv('store2_sales.csv', index=False)
    
    # Store 3 - Currency symbols in revenue
    print("Generating Store 3 data (with currency formatting)...")
    store3 = generate_store_sales('STORE_003', start_date, end_date, 230)
    store3['revenue'] = store3['revenue'].apply(
        lambda x: f"${x:,.2f}" if pd.notna(x) else x
    )
    store3.rename(columns={'product': 'product_name'}, inplace=True)
    store3.to_csv('store3_sales.csv', index=False)
    
    print("\n✓ Sample files generated successfully!")
    print("  - store1_sales.csv (250 records)")
    print("  - store2_sales.csv (220 records)")
    print("  - store3_sales.csv (230 records)")
    print("\nRun the ETL pipeline with:")
    print("  python etl/transform_sales.py")


if __name__ == "__main__":
    generate_sample_files()