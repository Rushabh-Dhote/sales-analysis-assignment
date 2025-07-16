WITH SALES_DATA AS (
    SELECT
        CDIM.gender,
        SFACT.product_line,
        COUNT(*) AS number_of_sales,
        ROW_NUMBER() OVER(PARTITION BY CDIM.gender ORDER BY COUNT(*) DESC) AS sales_ranking
    FROM
        SALES_FACT SFACT
    LEFT JOIN
        CUSTOMER_DIM CDIM
    ON
        SFACT.customer_key = CDIM.customer_key
    GROUP BY CDIM.gender, SFACT.product_line
)
SELECT
    gender, product_line, number_of_sales
FROM
    SALES_DATA
WHERE sales_ranking <=3
ORDER BY gender, sales_ranking