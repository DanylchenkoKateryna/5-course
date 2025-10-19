CREATE TABLE DimProduct (
    StockCode TEXT PRIMARY KEY,
    Description TEXT
);

CREATE TABLE DimCustomer (
    CustomerID INTEGER PRIMARY KEY,
    Country TEXT
);

CREATE TABLE DimDate (
    DateKey SERIAL PRIMARY KEY,
    InvoiceDate TIMESTAMP UNIQUE,
    Day INTEGER,
    Month INTEGER,
    Year INTEGER
);

CREATE TABLE FactSales (
    InvoiceNo TEXT,
    StockCode TEXT,
    CustomerID INTEGER,
    DateKey INTEGER,
    Quantity INTEGER,
    UnitPrice NUMERIC(10,2),
    TotalAmount NUMERIC(12,2) GENERATED ALWAYS AS (Quantity * UnitPrice) STORED,
    FOREIGN KEY (StockCode) REFERENCES DimProduct(StockCode),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);

CREATE TABLE staging_sales (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INTEGER,
    InvoiceDate TEXT,
    UnitPrice NUMERIC(10,2),
    CustomerID INTEGER,
    Country TEXT
);
