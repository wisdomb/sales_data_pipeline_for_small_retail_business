# Sales Data Pipeline for Retail Business

**Automated weekly sales consolidation system that saves 4-5 hours per week**

## The Problem

A small retail chain with 3 stores faced a critical bottleneck:
- Weekly sales data arrived in inconsistent CSV formats
- Manual consolidation took 4–5 hours every week
- Missing values and formatting issues caused errors
- Management couldn't get accurate monthly insights quickly

**Business Impact:** Delayed decisions, wasted staff time, potential revenue loss

## The Solution

Automated ETL pipeline that:
- ✅ Handles multiple CSV formats automatically
- ✅ Cleans and validates data with zero manual intervention
- ✅ Generates monthly summaries in under 2 minutes
- ✅ Loads results into PostgreSQL for instant querying
- ✅ Exports ready-to-use Excel reports for management

**Result:** 4+ hours saved weekly, instant access to accurate data

---

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐
│   Store 1   │──┐   │              │      │              │
│  (CSV File) │  │   │    Python    │      │  PostgreSQL  │
└─────────────┘  ├──▶│  ETL Script  │─────▶│   Database   │
┌─────────────┐  │   │   (pandas)   │      │              │
│   Store 2   │──┤   │              │      └──────────────┘
│  (CSV File) │  │   └──────────────┘              │
└─────────────┘  │          │                      │
┌─────────────┐  │          ▼                      ▼
│   Store 3   │──┘   ┌──────────────┐      ┌──────────────┐
│  (CSV File) │      │ Excel Report │      │   CSV Report │
└─────────────┘      └──────────────┘      └──────────────┘
```

---

## Features

### 1. **Smart Data Ingestion**
- Accepts multiple CSV files at once
- Handles different column names automatically (`date` / `sale_date` / `transaction_date`)
- Robust error handling for missing files

### 2. **Intelligent Data Cleaning**
- Converts dates to standard format
- Removes currency symbols from revenue (`$1,234.56` → `1234.56`)
- Eliminates invalid rows (missing dates, negative revenue)
- Logs all cleaning operations for audit trail

### 3. **Powerful Aggregations**
- Monthly totals by store
- Revenue, quantity, and order counts
- Year-over-year comparisons ready

### 4. **Database Integration**
- Auto-creates PostgreSQL tables
- Handles duplicate prevention
- Enables complex SQL queries for advanced analysis

### 5. **Professional Reporting**
- Excel workbook with multiple sheets (Summary, Raw Data, Overview)
- CSV exports for easy sharing
- Timestamp tracking for compliance

---

## Quick Start

### Prerequisites
```bash
pip install pandas sqlalchemy openpyxl psycopg2-binary
```

### Installation
```bash
git clone https://github.com/yourusername/sales-etl-pipeline.git
cd sales-etl-pipeline
pip install -r requirements.txt
```

### Generate Sample Data
```bash
python generate_sample_data.py
```

### Run the Pipeline
```python
from etl.transform_sales import SalesETL

# Without database (file output only)
etl = SalesETL()
etl.run_pipeline(['store1_sales.csv', 'store2_sales.csv', 'store3_sales.csv'])

# With PostgreSQL
etl = SalesETL("postgresql://user:password@localhost:5432/sales_db")
etl.run_pipeline(
    file_paths=['store1_sales.csv', 'store2_sales.csv'],
    load_db=True,
    export_reports=True
)
```

### Schedule Weekly Automation

**Linux/Mac (cron):**
```bash
# Run every Monday at 8 AM
0 8 * * 1 /usr/bin/python3 /path/to/run_etl.py
```

**Windows (Task Scheduler):**
Create a scheduled task pointing to `run_etl.py`

---

## Project Structure

```
sales-etl-pipeline/
│
├── etl/
│   └── transform_sales.py       # Main ETL script
│
├── reports/                     # Generated reports (auto-created)
│   ├── monthly_sales_summary_20241230.xlsx
│   └── monthly_sales_summary_20241230.csv
│
├── generate_sample_data.py      # Creates test data
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## Sample Output

### Excel Report Preview
**Sheet 1: Overview**
| Metric | Value |
|--------|-------|
| Total Revenue | $487,234.67 |
| Total Orders | 700 |
| Number of Stores | 3 |
| Date Range | 2024-10-01 to 2024-12-30 |

**Sheet 2: Monthly Summary**
| Store ID | Year-Month | Total Revenue | Total Orders |
|----------|------------|---------------|--------------|
| STORE_001 | 2024-10 | $45,234.21 | 78 |
| STORE_001 | 2024-11 | $52,100.45 | 89 |
| STORE_002 | 2024-10 | $38,900.12 | 65 |

---

## Database Schema

```sql
CREATE TABLE monthly_summary (
    id SERIAL PRIMARY KEY,
    store_id VARCHAR(50),
    year_month VARCHAR(7),
    total_revenue DECIMAL(12, 2),
    total_quantity INTEGER,
    total_orders INTEGER,
    report_generated TIMESTAMP,
    UNIQUE(store_id, year_month)
);
```

Query examples:
```sql
-- Top performing stores this year
SELECT store_id, SUM(total_revenue) as annual_revenue
FROM monthly_summary
WHERE year_month LIKE '2024%'
GROUP BY store_id
ORDER BY annual_revenue DESC;

-- Month-over-month growth
SELECT 
    year_month,
    SUM(total_revenue) as monthly_revenue,
    LAG(SUM(total_revenue)) OVER (ORDER BY year_month) as prev_month
FROM monthly_summary
GROUP BY year_month;
```

---

## Customization

### Add New Stores
Just drop new CSV files in the input directory. The pipeline automatically detects and processes them.

### Handle Different Column Names
Edit `column_mapping` in `transform_sales.py`:
```python
column_mapping = {
    'store_id': ['store_id', 'store', 'location_id', 'shop_id'],
    'revenue': ['revenue', 'sales', 'amount', 'total', 'income'],
}
```

### Change Aggregation Period
Modify aggregation logic from monthly to weekly/quarterly:
```python
# Weekly
df['year_week'] = df['date'].dt.strftime('%Y-W%U')

# Quarterly
df['year_quarter'] = df['date'].dt.to_period('Q').astype(str)
```

---

## Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python 3.8+** | Core programming language |
| **pandas** | Data manipulation and cleaning |
| **SQLAlchemy** | Database ORM |
| **PostgreSQL** | Data warehouse |
| **openpyxl** | Excel report generation |

---

## Testing

Run with sample data:
```bash
python generate_sample_data.py
python -m pytest tests/
```

---

## Future Enhancements

- [ ] Dashboard with real-time visualizations
- [ ] Email alerts for anomalies (sudden revenue drops)
- [ ] Integration with cloud storage (AWS S3, Google Drive)
- [ ] Machine learning forecasting for next month's sales
- [ ] Mobile app for managers to view reports on-the-go

---

## License

MIT License - feel free to use for your projects!

---

## Contact

**Wisdom Banda**  
Email: wisdombanda79@gmail.com
LinkedIn: linkedin.com/in/banda98
Portfolio: github.com/wisdomb

---

**⭐ If this helped you, please star the repo!**