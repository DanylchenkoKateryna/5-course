INSERT INTO DimProduct (StockCode, Description)
SELECT DISTINCT StockCode, Description
FROM staging_sales
ON CONFLICT (StockCode) DO NOTHING;

INSERT INTO DimCustomer (CustomerID, Country)
SELECT DISTINCT CustomerID, Country
FROM staging_sales
ON CONFLICT (CustomerID) DO NOTHING;

INSERT INTO DimDate (InvoiceDate, Day, Month, Year)
SELECT DISTINCT
    TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI') AS InvoiceDate,
    EXTRACT(DAY FROM TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI'))::INTEGER AS Day,
    EXTRACT(MONTH FROM TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI'))::INTEGER AS Month,
    EXTRACT(YEAR FROM TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI'))::INTEGER AS Year
FROM staging_sales
ON CONFLICT (InvoiceDate) DO NOTHING;

INSERT INTO FactSales (
    InvoiceNo,
    StockCode,
    CustomerID,
    DateKey,
    Quantity,
    UnitPrice
)
SELECT
    s.InvoiceNo,
    s.StockCode,
    s.CustomerID,
    d.DateKey,
    s.Quantity,
    s.UnitPrice
FROM staging_sales s
JOIN DimProduct p ON s.StockCode = p.StockCode
JOIN DimCustomer c ON s.CustomerID = c.CustomerID
JOIN DimDate d ON TO_TIMESTAMP(s.InvoiceDate, 'DD.MM.YYYY HH24:MI') = d.InvoiceDate;

