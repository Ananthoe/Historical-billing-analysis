ALTER TABLE cust_reg
MODIFY COLUMN INV_AMT decimal(15,2);
describe cust_reg;

-- Q1 Total Revenue by Segment
select cs.SEGMENT, sum(cr.INV_AMT)  
from cust_reg cr
join cust_seg cs on cr.CUSTOMER=cs.CUSTOMER 
group by cs.SEGMENT;

-- Q2 Monthly Revenue Trend (8 Quarters)
SELECT 
    CONCAT(
        'Y', 
        CASE 
            WHEN MONTH(DATE_TIME) >= 10 THEN YEAR(DATE_TIME) - 2021 
            ELSE YEAR(DATE_TIME) - 2022 
        END,
        'Q',
        CASE 
            WHEN MONTH(DATE_TIME) IN (10, 11, 12) THEN 1
            WHEN MONTH(DATE_TIME) IN (1, 2, 3) THEN 2
            WHEN MONTH(DATE_TIME) IN (4, 5, 6) THEN 3
            WHEN MONTH(DATE_TIME) IN (7, 8, 9) THEN 4
        END
    ) AS Quarter,
    SUM(INV_AMT) AS Total_Revenue
FROM 
    cust_reg
GROUP BY 
    CONCAT(
        'Y', 
        CASE 
            WHEN MONTH(DATE_TIME) >= 10 THEN YEAR(DATE_TIME) - 2021 
            ELSE YEAR(DATE_TIME) - 2022 
        END,
        'Q',
        CASE 
            WHEN MONTH(DATE_TIME) IN (10, 11, 12) THEN 1
            WHEN MONTH(DATE_TIME) IN (1, 2, 3) THEN 2
            WHEN MONTH(DATE_TIME) IN (4, 5, 6) THEN 3
            WHEN MONTH(DATE_TIME) IN (7, 8, 9) THEN 4
        END
        )
ORDER BY 
    Quarter;
    
-- Q2 Monthly Revenue Trend (Month and year)

SELECT 
    DATE_FORMAT(DATE_TIME, '%Y-%m') AS year_months, 
    SUM(INV_AMT) AS Total_Revenue
FROM cust_reg
GROUP BY DATE_FORMAT(DATE_TIME, '%Y-%m')  -- Group by year and month in 'YYYY-MM' format
ORDER BY DATE_FORMAT(DATE_TIME, '%Y-%m');  -- Orders results in chronological order

SELECT 
    CONCAT(MONTHNAME(DATE_TIME), '_', YEAR(DATE_TIME)) AS Month_Year, -- Concatenate month name and year
    SUM(INV_AMT) AS Total_Revenue
FROM cust_reg
WHERE DATE_TIME BETWEEN '2022-10-01' AND '2023-09-30'
GROUP BY CONCAT(MONTHNAME(DATE_TIME), '_', YEAR(DATE_TIME))
ORDER BY  CONCAT(MONTHNAME(DATE_TIME), '_', YEAR(DATE_TIME));

-- Q3 Segment-wise Customer Count
select SEGMENT, count(distinct CUSTOMER)
from cust_seg 
group by  SEGMENT;

-- Q4 Average Invoice Amount by Segment
select cs.segment, avg(inv_amt)
from cust_seg cs
join cust_reg cr on cs.CUSTOMER=cr.CUSTOMER
group by cs.segment; 

-- Q5 contribution by the top 20%
WITH RankedCustomers AS (
    SELECT 
        cs.segment,
        cr.CUSTOMER,
        SUM(cr.inv_amt) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY cs.segment ORDER BY SUM(cr.inv_amt) DESC) AS rn,
        COUNT(*) OVER (PARTITION BY cs.segment) AS total_customers,
        SUM(SUM(cr.inv_amt)) OVER (PARTITION BY cs.segment) AS segment_total_sales
    FROM 
        cust_seg cs
    JOIN 
        cust_reg cr ON cs.CUSTOMER = cr.CUSTOMER
	
    GROUP BY 
        cs.segment, cr.CUSTOMER
)
SELECT 
    segment,
    ROUND((SUM(total_sales) / segment_total_sales) * 100) AS contribution_percentage
FROM 
    RankedCustomers
WHERE 
    rn <= GREATEST(1, FLOOR(total_customers * 0.2))  -- Select the top 20% of customers in each segment

GROUP BY 
    segment, segment_total_sales  -- We group by segment to get total sales of top 20% of customers
ORDER BY 
    segment;
    
-- Q6 Count of single or multiple purchase type per segment.

SELECT 
    cs.segment,
    CASE 
        WHEN purchase_counts.purchase_count = 1 THEN 'One-Time Purchase'
        ELSE 'Repeat Purchase'
    END AS Purchase_Type,
    COUNT(*) AS Customer_Count
FROM 
    (
        SELECT 
            CUSTOMER, 
            COUNT(INV_AMT) AS purchase_count
        FROM 
            cust_reg
        GROUP BY 
            CUSTOMER
    ) AS purchase_counts
JOIN 
    cust_seg cs ON purchase_counts.CUSTOMER = cs.CUSTOMER
GROUP BY 
    cs.segment, 
    Purchase_Type
ORDER BY 
    cs.segment, 
    Purchase_Type DESC;
    
    -- USING CTE METHOD
    
WITH PurchaseCounts AS (
    -- Calculate purchase counts for each customer
    SELECT 
        CUSTOMER, 
        COUNT(INV_AMT) AS purchase_count
    FROM 
        cust_reg
    GROUP BY 
        CUSTOMER
),


SegmentedPurchases AS (
    -- Join purchase counts with segments
    SELECT 
        cs.segment,
        CASE 
            WHEN pc.purchase_count = 1 THEN 'One-Time Purchase'
            ELSE 'Repeat Purchase'
        END AS Purchase_Type
    FROM 
        PurchaseCounts pc
    JOIN 
        cust_seg cs ON pc.CUSTOMER = cs.CUSTOMER
)
-- Final aggregation
SELECT 
    segment,
    Purchase_Type,
    COUNT(*) AS Customer_Count
FROM 
    SegmentedPurchases
GROUP BY 
    segment, 
    Purchase_Type
ORDER BY 
    segment, 
    Purchase_Type DESC;

-- /



-- get the category name and total invoice amount for the category
select * from cust_reg;
select * from cust_seg;


select SEGMENT ,sum(INV_AMT) as Total from cust_seg cs
join cust_reg cr on cs.CUSTOMER= cr.CUSTOMER
group by SEGMENT;

-- list customer name whose total inv amount is greater than the average invoice amount 
select CUSTOMER, sum(INV_AMT)
from cust_reg
group by CUSTOMER
having sum(INV_AMT) > (select avg(INV_AMT) from cust_reg)
;

-- list customers whose total invoice is greater than the average invoice amt in their customer segment
 
 select  avg(INV_AMT) from cust_seg cs
 join cust_reg cr on cs.CUSTOMER = cr.CUSTOMER
 group by cs.SEGMENT;
 


-- list segment,customer, sum of total inv of customer whose total invoice is greater than the average invoice amt in their customer segment
select cs.SEGMENT, cr.CUSTOMER, sum(INV_AMT) as total_inv
from cust_reg cr
join cust_seg cs on cr.CUSTOMER = cs.CUSTOMER
group by cs.SEGMENT, cr.CUSTOMER
having total_inv > (select avg(INV_AMT) from cust_reg cr1
join cust_seg cs1 on cr1.CUSTOMER = cs1.CUSTOMER
where cs1.SEGMENT= cs.SEGMENT); 

select distinct cs.SEGMENT
from cust_seg cs;

select avg(INV_AMT) from cust_reg cr
join cust_seg cs1 on cr.CUSTOMER = cs1.CUSTOMER
where cs1.SEGMENT= "Medical";

-- give the total invoive value of all customers that are under either food or medical segment
select cr.CUSTOMER, sum(INV_AMT)
from cust_reg cr
join cust_seg cs on cr.CUSTOMER=cs.CUSTOMER 
where SEGMENT ="Food" OR SEGMENT= "Medical"
group by CUSTOMER;


-- invouce total for the customer
-- and invoice total for the segment  
-- entire  invoice total 
-- total count of customers,
-- count of customers in the segments
-- display count of customers from all segment except the current segment, display count of customers from the
-- segment where the count is maximum across all segments 

select  cr.CUSTOMER, cs.SEGMENT,
        sum(INV_AMT) over (partition by cr.CUSTOMER ) as Cust_total ,
		sum(INV_AMT) over (partition by cs.SEGMENT ) as segment_total,
        sum(INV_AMT) over ()  Grand_tot,
        (select count(distinct CUSTOMER) from cust_reg ) as cnt_distinct_cust,
        (select count(distinct CUSTOMER) from cust_seg cs1 where cs1.SEGMENT = cs.SEGMENT ) as seg_cust_cnt,
      (    SELECT MAX(distinct_count)
FROM (
    SELECT COUNT(DISTINCT CUSTOMER) AS distinct_count
    FROM cust_seg
    GROUP BY SEGMENT
) AS subquery ) as max_seg_cust_cnt
from cust_reg cr
join cust_seg cs on cr.CUSTOMER= cs.CUSTOMER;

select count(distinct customer) from cust_reg;

Select SEGMENT, count(distinct CUSTOMER) from cust_seg group by SEGMENT; -- distinct customers in the segment


SELECT MAX(distinct_count)
FROM (
    SELECT COUNT(DISTINCT CUSTOMER) AS distinct_count
    FROM cust_seg
    GROUP BY SEGMENT
) AS subquery;

-- display customer and average sales for all customers whose average sales is greater than overall average sales

select CUSTOMER, avg(INV_AMT) as avg_inv
from cust_reg
group by CUSTOMER
having avg_inv> (select avg(INV_AMT) from cust_reg);

select avg(INV_AMT) from cust_reg;

-- display customer and total sales for all customers whose total sales is greater than segment average sales

select cs.SEGMENT, cr.CUSTOMER, sum(INV_AMT) as total_inv	
from cust_reg cr	
join cust_seg cs on cr.CUSTOMER = cs.CUSTOMER	
group by cs.SEGMENT, cr.CUSTOMER	
having total_inv > 
(   select avg(INV_AMT) as avg_amt from cust_reg cr1	
	join cust_seg cs1 on cr1.CUSTOMER = cs1.CUSTOMER	
	where cs1.SEGMENT= cs.SEGMENT
      );	

-- list customers whose average invoice is greater than the average invoice amt in their customer segment
select cs.SEGMENT, cr.CUSTOMER, avg(INV_AMT) as total_inv
from cust_reg cr
join cust_seg cs on cr.CUSTOMER = cs.CUSTOMER
group by cs.SEGMENT, cr.CUSTOMER
having total_inv > (select avg(INV_AMT) from cust_reg cr
join cust_seg cs1 on cr.CUSTOMER = cs1.CUSTOMER
where cs1.SEGMENT= cs.SEGMENT); 

-- another way using cte for the sum greater than avg of segment Question
WITH SegmentAvg AS (
    SELECT cs1.SEGMENT, AVG(cr1.INV_AMT) AS avg_sales
    FROM cust_reg cr1
    JOIN cust_seg cs1 ON cr1.CUSTOMER = cs1.CUSTOMER
    GROUP BY cs1.SEGMENT
),
CustomerSales AS (
    SELECT cs.SEGMENT, cr.CUSTOMER, SUM(cr.INV_AMT) AS total_inv
    FROM cust_reg cr
    JOIN cust_seg cs ON cr.CUSTOMER = cs.CUSTOMER
    GROUP BY cs.SEGMENT, cr.CUSTOMER
)
SELECT cs.SEGMENT, cs.CUSTOMER, cs.total_inv
FROM CustomerSales cs
JOIN SegmentAvg sa ON cs.SEGMENT = sa.SEGMENT
WHERE cs.total_inv > sa.avg_sales;
 -- _________________________________________________________________ cte alone
 with customersales as (
	select cs1.SEGMENT, cr1.CUSTOMER, sum(INV_AMT) as tot_inv
    from cust_seg cs1
    join cust_reg cr1 on cs1.CUSTOMER=cr1.CUSTOMER
    group by cs1.SEGMENT, cr1.CUSTOMER
 )
 ,segmentaverage as (
	select cs2.SEGMENT, avg(INV_AMT) as seg_avg
    from cust_reg cr2
    join cust_seg cs2 on cr2.CUSTOMER=cs2.CUSTOMER
    group by cs2.SEGMENT
 )
 select cs.customer, sa.SEGMENT, tot_inv from 
 segmentaverage sa
 join customersales cs on cs.SEGMENT= sa.SEGMENT
 where cs.tot_inv > sa.seg_avg;
 
  -- _________________________________________________________________ correlated subquery alone
  -- display customer and total sales for all customers whose total sales is greater than segment average sales
  
  select cr.CUSTOMER, sum(INV_AMT) as tot_inv
  from cust_reg cr
  join cust_seg cs on cr.CUSTOMER=cs.CUSTOMER
  group by cr.customer, cs.SEGMENT
  having tot_inv> (select avg(inv_amt) from cust_reg cr1
  join cust_seg cs1 on cr1.CUSTOMER=cs1.CUSTOMER
  where cs1.SEGMENT=cs.SEGMENT);
  
  -- Display the maximum inv
select CUSTOMER,max(inv_amt) , min(INV_AMT) from cust_reg;
  
  -- display the customr/s (name) who has the maximum sales value
  select CUSTOMER, max(inv_amt) from cust_reg
  group by CUSTOMER;
  
SELECT customer, INV_AMT from cust_reg
where INV_AMT = ( select  max(inv_amt) from cust_reg);

-- Display the customer having the max invoice value and his segment
SELECT cr.customer, cs.SEGMENT,INV_AMT from cust_reg cr
join cust_seg cs on cr.CUSTOMER=cs.CUSTOMER
where INV_AMT = ( select  max(inv_amt) from cust_reg);

-- display customer name, max of invoice val for the customer, segment name, average inv for segment
SELECT cr.customer, cs.SEGMENT,
		(select max(INV_AMT) from cust_reg cr1 where cr1.CUSTOMER= cr.CUSTOMER) as cust_max,
		avg(INV_AMT) over (partition by SEGMENT) as seg_Avg
from cust_reg cr
join cust_seg cs on cr.CUSTOMER=cs.CUSTOMER;

-- display the name of the segment who has the maximum average invoice
select seg.SEGMENT,max(seg_avg) AS max_seg_avg 
from
(
select SEGMENT, avg(INV_AMT) as seg_avg from
cust_reg cr
join cust_seg cs on cr.CUSTOMER=cs.CUSTOMER
group by SEGMENT) as seg
group by seg.SEGMENT;


-- segment and segment average
select * from cust_reg;

select SEGMENT, avg(INV_AMT) from
cust_reg cr
join cust_seg cs on cr.CUSTOMER= cs.CUSTOMER
group by SEGMENT;
-- display segments that has atleast 5 customer


select cs.SEGMENT
from cust_reg cr
join cust_seg cs on cr.CUSTOMER = cs.CUSTOMER
group by cs.SEGMENT
having count( distinct cr.CUSTOMER) >= 5;



