INSERT INTO DimCountry (CountryName)
SELECT DISTINCT Country
FROM staging_sales
WHERE Country IS NOT NULL AND Country <> '';

INSERT INTO DimCustomer (CustomerID, CountryID)
SELECT DISTINCT
    s.CustomerID,
    c.CountryID
FROM staging_sales s
JOIN DimCountry c ON s.Country = c.CountryName
ON CONFLICT (CustomerID) DO NOTHING;

INSERT INTO DimProduct (StockCode, Description)
SELECT DISTINCT StockCode, Description
FROM staging_sales
WHERE StockCode IS NOT NULL AND Description IS NOT NULL
ON CONFLICT (StockCode) DO NOTHING;

INSERT INTO DimDate (InvoiceDate, Day, Month, Year)
SELECT DISTINCT
    TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI'),
    EXTRACT(DAY FROM TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI'))::INTEGER,
    EXTRACT(MONTH FROM TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI'))::INTEGER,
    EXTRACT(YEAR FROM TO_TIMESTAMP(InvoiceDate, 'DD.MM.YYYY HH24:MI'))::INTEGER
FROM staging_sales
WHERE InvoiceDate IS NOT NULL;

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
JOIN DimCustomer cu ON s.CustomerID = cu.CustomerID
JOIN DimProduct p ON s.StockCode = p.StockCode
JOIN DimDate d ON TO_TIMESTAMP(s.InvoiceDate, 'DD.MM.YYYY HH24:MI') = d.InvoiceDate;

