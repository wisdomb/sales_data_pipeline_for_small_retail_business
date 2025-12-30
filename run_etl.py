from etl.transform_sales import SalesETL

etl = SalesETL()
etl.run_pipeline(
    file_paths=['store1_sales.csv', 'store2_sales.csv', 'store3_sales.csv'],
    export_reports=True,
    load_db=False
)