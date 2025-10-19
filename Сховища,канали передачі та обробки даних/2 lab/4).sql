WITH RECURSIVE category_hierarchy AS (
    SELECT 
        category_id,
        category_name,
        parent_id,
        category_name::TEXT AS category_path,
        1 AS level
    FROM categories
    WHERE parent_id IS NULL
    
    UNION ALL
	
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_id,
        CONCAT(ch.category_path, ' > ', c.category_name)::TEXT AS category_path,
        ch.level + 1 AS leve
    FROM categories c
    INNER JOIN category_hierarchy ch ON c.parent_id = ch.category_id
)
SELECT 
    p.product_id,
    p.product_name,
    ch.category_path,
    ch.level,
    p.price
FROM products p
INNER JOIN category_hierarchy ch ON p.category_id = ch.category_id
ORDER BY ch.category_path, p.product_name;