-- Snowflake skrypt do testowania
USE WAREHOUSE COMPUTE_XS_WH;
USE WAREHOUSE COMPUTE_S_WH;
USE WAREHOUSE COMPUTE_M_WH;
USE WAREHOUSE COMPUTE_L_WH;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Query testowe 1

WITH LargeOrders AS (  
    SELECT  
        o.o_orderkey,  
        SUM(l.l_quantity) AS total_quantity  
    FROM  
        SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.orders o  
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.lineitem l ON o.o_orderkey = l.l_orderkey  
    GROUP BY  
        o.o_orderkey  
    HAVING  
        SUM(l.l_quantity) > 50
),  
OrderDetails AS (  
    SELECT  
        lo.o_orderkey,  
        o.o_orderdate,  
        o.o_totalprice,  
        c.c_name,  
        c.c_nationkey  
    FROM  
        LargeOrders lo  
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.orders o ON lo.o_orderkey = o.o_orderkey  
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.customer c ON o.o_custkey = c.c_custkey  
)  
SELECT  
    od.*,  
    n.n_name AS nation  
FROM  
    OrderDetails od  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.nation n ON od.c_nationkey = n.n_nationkey  
ORDER BY  
    od.o_totalprice DESC


-- Query testowe 2
WITH sales AS (  
    SELECT  
        l.l_orderkey,  
        l.l_partkey,  
        o.o_orderdate,  
        TRUNC(o.o_orderdate, 'month')  AS order_month,  
        o.o_custkey,  
        c.c_nationkey,  
        l.l_quantity,  
        l.l_extendedprice * (1 - l.l_discount) AS sales,  
        ps.ps_supplycost * l.l_quantity AS cost,  
        (l.l_extendedprice * (1 - l.l_discount))  
          - (ps.ps_supplycost * l.l_quantity)   AS margin  
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.lineitem  l  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.orders    o  ON o.o_orderkey = l.l_orderkey  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.customer  c  ON c.c_custkey = o.o_custkey  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.partsupp  ps ON ps.ps_partkey = l.l_partkey  
                     AND ps.ps_suppkey = l.l_suppkey  
    WHERE o.o_orderdate BETWEEN '1994-01-01' AND '1997-12-31'  
),  
metrics AS (  
    SELECT  
        *,  
        SUM(sales)  OVER (PARTITION BY c_nationkey, order_month) AS nation_month_sales,  
        SUM(margin) OVER (PARTITION BY c_nationkey  ORDER BY order_month  RANGE BETWEEN INTERVAL '11 MONTH' PRECEDING AND CURRENT ROW) AS nation_margin_last_year ,
        AVG(margin) OVER (PARTITION BY l_partkey) AS part_avg_margin,  
        ROW_NUMBER() OVER (PARTITION BY c_nationkey, order_month ORDER BY sales DESC) AS row_num_sales,  
        RANK()       OVER (PARTITION BY c_nationkey ORDER BY margin DESC) AS margin_rank_nation,  
        DENSE_RANK() OVER (PARTITION BY order_month  ORDER BY sales DESC) AS dense_rank_sales_month,  
        PERCENT_RANK() OVER (PARTITION BY order_month ORDER BY margin) AS pct_rank_margin_month,  
        CUME_DIST()   OVER (PARTITION BY l_partkey   ORDER BY margin DESC) AS cume_margin_part,  
        LAG (sales, 1, 0) OVER (PARTITION BY l_partkey ORDER BY order_month) AS prev_month_sales_part,  
        LEAD(sales, 1, 0) OVER (PARTITION BY l_partkey ORDER BY order_month) AS next_month_sales_part  
    FROM sales  
)  
SELECT  
    c_nationkey,  
    order_month,  
    SUM(sales)  AS total_sales,  
    SUM(cost)   AS total_cost,  
    SUM(margin) AS total_margin,  
    AVG(part_avg_margin)         AS avg_part_margin,  
    MAX(nation_margin_last_year) AS rolling_year_margin,  
    COUNT_IF(row_num_sales <= 5) AS top5_orders_in_month,  
    MIN(pct_rank_margin_month)   AS best_pct_rank_margin,  
    MAX(cume_margin_part)        AS max_cume_margin  
FROM metrics  
GROUP BY c_nationkey, order_month  
ORDER BY total_margin DESC  
LIMIT 100;


------ SF100GB


-- Query testowe 1

WITH LargeOrders AS (  
    SELECT  
        o.o_orderkey,  
        SUM(l.l_quantity) AS total_quantity  
    FROM  
        SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.orders o  
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.lineitem l ON o.o_orderkey = l.l_orderkey  
    GROUP BY  
        o.o_orderkey  
    HAVING  
        SUM(l.l_quantity) > 50
),  
OrderDetails AS (  
    SELECT  
        lo.o_orderkey,  
        o.o_orderdate,  
        o.o_totalprice,  
        c.c_name,  
        c.c_nationkey  
    FROM  
        LargeOrders lo  
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.orders o ON lo.o_orderkey = o.o_orderkey  
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.customer c ON o.o_custkey = c.c_custkey  
)  
SELECT  
    od.*,  
    n.n_name AS nation  
FROM  
    OrderDetails od  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.nation n ON od.c_nationkey = n.n_nationkey  
ORDER BY  
    od.o_totalprice DESC


-- Query testowe 2
WITH sales AS (  
    SELECT  
        l.l_orderkey,  
        l.l_partkey,  
        o.o_orderdate,  
        TRUNC(o.o_orderdate, 'month')  AS order_month,  
        o.o_custkey,  
        c.c_nationkey,  
        l.l_quantity,  
        l.l_extendedprice * (1 - l.l_discount) AS sales,  
        ps.ps_supplycost * l.l_quantity AS cost,  
        (l.l_extendedprice * (1 - l.l_discount))  
          - (ps.ps_supplycost * l.l_quantity)   AS margin  
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.lineitem  l  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.orders    o  ON o.o_orderkey = l.l_orderkey  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.customer  c  ON c.c_custkey = o.o_custkey  
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.partsupp  ps ON ps.ps_partkey = l.l_partkey  
                     AND ps.ps_suppkey = l.l_suppkey  
    WHERE o.o_orderdate BETWEEN '1994-01-01' AND '1997-12-31'  
),  
metrics AS (  
    SELECT  
        *,  
        SUM(sales)  OVER (PARTITION BY c_nationkey, order_month) AS nation_month_sales,  
        SUM(margin) OVER (PARTITION BY c_nationkey  ORDER BY order_month  RANGE BETWEEN INTERVAL '11 MONTH' PRECEDING AND CURRENT ROW) AS nation_margin_last_year ,
        AVG(margin) OVER (PARTITION BY l_partkey) AS part_avg_margin,  
        ROW_NUMBER() OVER (PARTITION BY c_nationkey, order_month ORDER BY sales DESC) AS row_num_sales,  
        RANK()       OVER (PARTITION BY c_nationkey ORDER BY margin DESC) AS margin_rank_nation,  
        DENSE_RANK() OVER (PARTITION BY order_month  ORDER BY sales DESC) AS dense_rank_sales_month,  
        PERCENT_RANK() OVER (PARTITION BY order_month ORDER BY margin) AS pct_rank_margin_month,  
        CUME_DIST()   OVER (PARTITION BY l_partkey   ORDER BY margin DESC) AS cume_margin_part,  
        LAG (sales, 1, 0) OVER (PARTITION BY l_partkey ORDER BY order_month) AS prev_month_sales_part,  
        LEAD(sales, 1, 0) OVER (PARTITION BY l_partkey ORDER BY order_month) AS next_month_sales_part  
    FROM sales  
)  
SELECT  
    c_nationkey,  
    order_month,  
    SUM(sales)  AS total_sales,  
    SUM(cost)   AS total_cost,  
    SUM(margin) AS total_margin,  
    AVG(part_avg_margin)         AS avg_part_margin,  
    MAX(nation_margin_last_year) AS rolling_year_margin,  
    COUNT_IF(row_num_sales <= 5) AS top5_orders_in_month,  
    MIN(pct_rank_margin_month)   AS best_pct_rank_margin,  
    MAX(cume_margin_part)        AS max_cume_margin  
FROM metrics  
GROUP BY c_nationkey, order_month  
ORDER BY total_margin DESC  
LIMIT 100;