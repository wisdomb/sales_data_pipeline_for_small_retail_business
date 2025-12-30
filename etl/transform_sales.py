import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import logging
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SalesETL:
    """ETL pipeline for retail sales data"""
    
    def __init__(self, db_connection_string=None):
        self.db_connection_string = db_connection_string
        self.raw_data = []
        self.cleaned_data = None
        self.monthly_summary = None
        
    def extract(self, file_paths):
        """
        Extract data from CSV files
        
        Args:
            file_paths: List of CSV file paths or single path
        """
        logger.info("Starting data extraction...")
        
        if isinstance(file_paths, (str, Path)):
            file_paths = [file_paths]
            
        for file_path in file_paths:
            try:
                df = pd.read_csv(file_path)
                logger.info(f"Loaded {len(df)} rows from {file_path}")
                self.raw_data.append(df)
            except Exception as e:
                logger.error(f"Failed to load {file_path}: {e}")
                
        logger.info(f"Extraction complete: {len(self.raw_data)} files loaded")
        
    def transform(self):
        """Clean and transform sales data"""
        logger.info("Starting data transformation...")
        
        if not self.raw_data:
            logger.error("No data to transform")
            return
            
        # Combine all dataframes
        df = pd.concat(self.raw_data, ignore_index=True)
        logger.info(f"Combined data: {len(df)} total rows")
        
        # Standardize column names (handle variations)
        df.columns = df.columns.str.lower().str.strip()
        
        # Common column mapping for different formats
        column_mapping = {
            'store_id': ['store_id', 'store', 'location_id'],
            'date': ['date', 'sale_date', 'transaction_date'],
            'revenue': ['revenue', 'sales', 'amount', 'total'],
            'product': ['product', 'product_name', 'item'],
            'quantity': ['quantity', 'qty', 'units']
        }
        
        # Apply column mapping
        for standard_name, possible_names in column_mapping.items():
            for col in df.columns:
                if col in possible_names:
                    df.rename(columns={col: standard_name}, inplace=True)
                    break
        
        # Data cleaning
        logger.info("Cleaning data...")
        
        # Convert date to datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
        # Convert revenue to numeric
        if 'revenue' in df.columns:
            df['revenue'] = pd.to_numeric(
                df['revenue'].astype(str).str.replace('[$,]', '', regex=True),
                errors='coerce'
            )
            
        # Convert quantity to numeric
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
            
        # Remove rows with missing critical data
        initial_count = len(df)
        df = df.dropna(subset=['date', 'revenue', 'store_id'])
        removed = initial_count - len(df)
        logger.info(f"Removed {removed} invalid rows")
        
        # Add derived columns
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['year_month'] = df['date'].dt.to_period('M').astype(str)

        self.cleaned_data = df
        logger.info(f"Transformation complete: {len(df)} clean rows")

    def aggregate(self):
        """Create monthly aggregations"""
        logger.info("Creating monthly aggregations...")

        if self.cleaned_data is None:
            logger.error("No cleaned data available")
            return

        # Monthly summary by store
        monthly = self.cleaned_data.groupby(['store_id', 'year_month']).agg({
            'revenue': 'sum',
            'quantity': 'sum',
            'date': 'count'  # Number of transactions
        }).reset_index()

        monthly.columns = ['store_id', 'year_month', 'total_revenue', 
                        'total_quantity', 'total_orders']

        # Round revenue to 2 decimals
        monthly['total_revenue'] = monthly['total_revenue'].round(2)

        # Add summary date
        monthly['report_generated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.monthly_summary = monthly
        logger.info(f"Aggregation complete: {len(monthly)} monthly records")

    def load_to_database(self):
        """Load data to PostgreSQL"""
        if not self.db_connection_string:
            logger.warning("No database connection provided, skipping database load")
            return

        logger.info("Loading data to PostgreSQL...")

        try:
            engine = create_engine(self.db_connection_string)

            # Create table if not exists
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS monthly_summary (
                id SERIAL PRIMARY KEY,
                store_id VARCHAR(50),
                year_month VARCHAR(7),
                total_revenue DECIMAL(12, 2),
                total_quantity INTEGER,
                total_orders INTEGER,
                report_generated TIMESTAMP,
                UNIQUE(store_id, year_month)
            );
            """
            with engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()

            # Load data (replace existing records for same store/month)
            self.monthly_summary.to_sql(
                'monthly_summary',
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )

            logger.info("Database load complete")

        except Exception as e:
            logger.error(f"Database load failed: {e}")

    def export_reports(self, output_dir='reports'):
        """Export summary reports as CSV and Excel"""
        logger.info("Exporting reports...")

        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # CSV report
        csv_file = output_path / f'monthly_sales_summary_{timestamp}.csv'
        self.monthly_summary.to_csv(csv_file, index=False)
        logger.info(f"CSV report saved: {csv_file}")

        # Excel report with formatting
        excel_file = output_path / f'monthly_sales_summary_{timestamp}.xlsx'

        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Summary sheet
            self.monthly_summary.to_excel(
                writer,
                sheet_name='Monthly Summary',
                index=False
            )

            # Raw data sheet
            self.cleaned_data.to_excel(
                writer,
                sheet_name='Raw Data',
                index=False
            )

            # Overall summary
            overall = pd.DataFrame({
                'Metric': ['Total Revenue', 'Total Orders', 'Total Quantity', 
                        'Number of Stores', 'Date Range'],
                'Value': [
                    f"${self.cleaned_data['revenue'].sum():,.2f}",
                    f"{len(self.cleaned_data):,}",
                    f"{self.cleaned_data['quantity'].sum():,.0f}",
                    self.cleaned_data['store_id'].nunique(),
                    f"{self.cleaned_data['date'].min().date()} to {self.cleaned_data['date'].max().date()}"
                ]
            })
            overall.to_excel(writer, sheet_name='Overview', index=False)

        logger.info(f"Excel report saved: {excel_file}")

    def run_pipeline(self, file_paths, export_reports=True, load_db=False):

        logger.info("=" * 60)
        logger.info("SALES ETL PIPELINE STARTED")
        logger.info("=" * 60)

        self.extract(file_paths)
        self.transform()
        self.aggregate()

        if load_db:
            self.load_to_database()

        if export_reports:
            self.export_reports()

        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

if __name__ == "__main__":
    print("Sales ETL Pipeline ready to use!")
    print("\nQuick Start:")
    print("1. etl = SalesETL()")
    print("2. etl.run_pipeline(['sales_file1.csv', 'sales_file2.csv'])")
    print("\nFor database loading:")
    print("etl = SalesETL('postgresql://user:pass@host:5432/db')")
    print("etl.run_pipeline(files, load_db=True)")