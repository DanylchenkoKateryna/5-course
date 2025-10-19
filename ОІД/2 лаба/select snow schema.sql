SELECT
    p.Description,
	ct.CountryName,
    SUM(f.Quantity) AS TotalQuantity,
    ROUND(SUM(f.TotalAmount), 2) AS TotalRevenue
FROM FactSales f
JOIN DimCustomer c ON f.CustomerId = c.CustomerId
JOIN DimCountry ct ON c.CountryId = ct.CountryId
JOIN DimProduct p ON f.StockCode = p.StockCode
GROUP BY ct.CountryName, p.Description
having ct.CountryName = 'United Kingdom'
ORDER BY TotalQuantity DESC
LIMIT 1


