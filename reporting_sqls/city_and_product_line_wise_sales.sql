--
SELECT
    bdim.city,
    sfact.product_line,
    SUM(sfact.sales) AS total_sales,
    SUM(sfact.gross_income) AS total_gross_income
FROM
    sales_fact sfact
JOIN
    branch_dim bdim
ON sfact.branch_key = bdim.branch_key
GROUP BY
    bdim.city, sfact.product_line
ORDER BY
    bdim.city, total_sales DESC;
