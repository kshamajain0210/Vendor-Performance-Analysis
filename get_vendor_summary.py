import pandas as pd
import logging
from ingestion_db import ingest_db, engine

# ---------- Logging Setup ----------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ---------- Step 1: Create Vendor Summary ----------
def create_vendor_summary(engine):
    """Merge tables and return vendor summary DataFrame."""
    try:
        query = """
        WITH FreightSummary AS
        (
            SELECT VendorNumber, SUM(Freight) AS FreightCost 
            FROM vendor_invoice 
            GROUP BY VendorNumber
        ),
        PurchaseSummary AS 
        (
            SELECT 
                p.VendorNumber,
                p.VendorName,
                p.Brand,
                p.Description,
                p.PurchasePrice,
                pp.Volume,
                pp.Price AS ActualPrice,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp
                ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, 
                     p.PurchasePrice, pp.Price, pp.Volume
        ),
        SalesSummary AS
        (
            SELECT
                VendorNo AS VendorNumber,
                SalesPrice,
                Brand,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        )
        SELECT
            ps.VendorNumber, ps.VendorName, ps.Brand,
            ps.Description, ps.PurchasePrice, ps.ActualPrice, ps.Volume,
            ps.TotalPurchaseQuantity, ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity, ss.TotalSalesDollars, ss.TotalSalesPrice, ss.TotalExciseTax, 
            fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNumber
           AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber 
        ORDER BY ps.TotalPurchaseDollars DESC
        """

        with engine.connect() as conn:
            vendor_summary = pd.read_sql_query(query, conn)
            logging.info("Vendor summary created successfully.")
        return vendor_summary

    except Exception as e:
        logging.error(f"Error creating vendor summary: {e}", exc_info=True)
        raise


# ---------- Step 2: Clean Data ----------
def clean_data(df):
    """Clean vendor summary data."""
    try:
        df['Volume'] = df['Volume'].astype('float64')
        df.fillna(0, inplace=True)
        df['VendorName'] = df['VendorName'].str.strip()
        df['Description'] = df['Description'].str.strip()

        df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
        df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'].replace(0, pd.NA)) * 100
        df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, pd.NA)
        df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, pd.NA)

        logging.info("Data cleaning complete.")
        return df

    except Exception as e:
        logging.error(f"Error cleaning data: {e}", exc_info=True)
        raise


# ---------- Step 3: Run Full ETL ----------
if __name__ == '__main__':
    try:
        logging.info("========== Vendor Summary ETL Started ==========")
        summary_df = create_vendor_summary(engine)
        clean_df = clean_data(summary_df)
        ingest_db(clean_df, 'vendor_sales_summary', engine)
        logging.info("========== Vendor Summary ETL Completed ==========")
    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)




