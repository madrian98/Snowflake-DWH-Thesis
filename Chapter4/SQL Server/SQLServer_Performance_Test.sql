-- Skrypt do zaimportowania danych TPC-H z plików CSV  
USE TESTY_CH4;

--#####################################
-- Usuwanie pamiêci podrêcznej 
--#####################################

CHECKPOINT;  
DBCC DROPCLEANBUFFERS;  
DBCC FREEPROCCACHE;


-- Statystyki
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

--#####################################
-- Query testowe
--#####################################

EXEC sp_updatestats;


-- Query 1

WITH LargeOrders AS (  
    SELECT  
        o.o_orderkey,  
        SUM(l.l_quantity) AS total_quantity  
    FROM  
        orders o  
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey  
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
        JOIN orders o ON lo.o_orderkey = o.o_orderkey  
        JOIN customer c ON o.o_custkey = c.c_custkey  
)  
SELECT  
    od.*,  
    n.n_name AS nation  
FROM  
    OrderDetails od  
    JOIN nation n ON od.c_nationkey = n.n_nationkey  
ORDER BY  
    od.o_totalprice DESC





-- Query 2

WITH sales AS (  
    SELECT  
        l.l_orderkey,  
        l.l_partkey,  
        o.o_orderdate,  
        DATEADD(MONTH, DATEDIFF(MONTH, 0, o.o_orderdate), 0) AS order_month,  
        o.o_custkey,  
        c.c_nationkey,  
        l.l_quantity,  
        l.l_extendedprice * (1 - l.l_discount) AS sales,  
        ps.ps_supplycost * l.l_quantity  AS cost,  
        (l.l_extendedprice * (1 - l.l_discount))  
          - (ps.ps_supplycost * l.l_quantity)  AS margin  
    FROM lineitem  l  
    JOIN orders    o  ON o.o_orderkey = l.l_orderkey  
    JOIN customer  c  ON c.c_custkey = o.o_custkey  
    JOIN partsupp  ps ON ps.ps_partkey = l.l_partkey  
                     AND ps.ps_suppkey = l.l_suppkey  
    WHERE o.o_orderdate BETWEEN '1994-01-01' AND '1997-12-31'  
),  
metrics AS (  
    SELECT  
        *,  
        SUM(sales)  OVER (PARTITION BY c_nationkey, order_month) AS nation_month_sales, 
        SUM(margin) OVER (PARTITION BY c_nationkey ORDER BY order_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS nation_margin_last_year,  
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
    AVG(part_avg_margin)           AS avg_part_margin,  
    MAX(nation_margin_last_year)   AS rolling_year_margin,  
    SUM(CASE WHEN row_num_sales <= 5 THEN 1 END) AS top5_orders_in_month,  
    MIN(pct_rank_margin_month)      AS best_pct_rank_margin,  
    MAX(cume_margin_part)           AS max_cume_margin  
FROM metrics  
GROUP BY c_nationkey, order_month  
ORDER BY total_margin DESC  
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY;