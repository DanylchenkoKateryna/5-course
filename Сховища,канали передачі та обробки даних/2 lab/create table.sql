CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_id INT,

	FOREIGN KEY (parent_id) REFERENCES categories(category_id)
        ON DELETE SET NULL
);

INSERT INTO categories(category_id,category_name,parent_id)
VALUES (1, 'Furniture', NULL),
(2, 'Home & Kitchen', 1),
(3, 'Electronics', NULL),
(4, 'Accessories', 3)

ALTER TABLE products
ADD COLUMN category_id INT;

ALTER TABLE products
ADD FOREIGN KEY (category_id) REFERENCES categories(category_id);

UPDATE products
SET category_id = 1
WHERE category = 'Furniture';

UPDATE products
SET category_id = 2
WHERE category = 'Home & Kitchen';

UPDATE products
SET category_id = 3
WHERE category = 'Electronics';

UPDATE products
SET category_id = 4
WHERE category = 'Accessories';

ALTER TABLE products
DROP COLUMN category;
