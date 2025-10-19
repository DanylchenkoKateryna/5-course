EXPLAIN ANALYZE
SELECT
    p.Description,
	c.Country,
    SUM(f.Quantity) AS TotalQuantity,
    ROUND(SUM(f.TotalAmount), 2) AS TotalRevenue
FROM FactSales f
JOIN DimCustomer c ON f.CustomerId = c.CustomerId
JOIN DimProduct p ON f.StockCode = p.StockCode
GROUP BY c.Country, p.Description
having c.Country = 'United Kingdom'
ORDER BY TotalQuantity DESC
LIMIT 1


