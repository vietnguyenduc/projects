--Lưu ý chung: với Bigquery thì mình có thể groupby, orderby 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé

--Cau 1:
SELECT 
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month_extract,
    SUM(totals.visits) AS visits,
    SUM(totals.pageviews) AS pageviews,
    SUM(totals.transactions) AS transactions -- transactions per session
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month_extract;
--correct

-- Cau 2:
SELECT 
    trafficSource.source AS page_source,
    SUM(totals.visits) AS total_visits,
    SUM(totals.bounces) AS total_no_of_bounces,
    SUM(totals.bounces)/SUM(totals.visits)*100.0 AS bounce_rate -- no round required
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` -- in July 2017
GROUP BY page_source;
--correct


-- Cau 3: 
WITH month_range AS
(
    SELECT 
        'Month' AS time_type
        ,FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS time
        ,trafficSource.source AS source
        ,SUM(product.productRevenue) AS revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,  -- semicolon after FROM to use UNNEST
    UNNEST (hits) AS hits
    ,UNNEST (hits.product) AS product
    WHERE productRevenue is not null
    GROUP BY time, source
)
    ,week_range AS
(
    SELECT 
        'Week' AS time_type
        ,FORMAT_DATE("%Y%W",PARSE_DATE("%Y%m%d",date)) AS time --%W:The week number of the year (Monday as the first day of the week) as a decimal number (00-53).
        ,trafficSource.source AS source
        ,SUM(product.productRevenue) AS revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,  -- semicolon after FROM to use UNNEST
    UNNEST (hits) AS hits
    ,UNNEST (hits.product) AS product
    WHERE productRevenue is not null
    GROUP BY time, source
)
SELECT *
FROM month_range
UNION ALL
SELECT *
FROM week_range;
--correct

-- Cau 4: 
WITH id_status AS
(
    SELECT 
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
        ,fullVisitorId AS id
        ,product.productRevenue AS revenue
        ,totals.pageviews AS pageviews
        ,totals.transactions AS transactions
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
        ,UNNEST (hits) AS hits
        ,UNNEST (hits.product) AS product
    WHERE _table_suffix BETWEEN '0601' AND '0731'
     
)
    , p_num AS
(
    SELECT
        month
        ,COUNT(DISTINCT id) AS purchaser
        ,SUM(pageviews) AS p_pageviews
    FROM id_status
    WHERE transactions is not null -- transactions instead of revenue??? maybe transaction included null revenue...
    GROUP BY month
)
    , non_p_num AS
(
    SELECT
        month
        ,COUNT(DISTINCT id) AS non_purchaser
        ,SUM(pageviews) AS non_p_pageviews
    FROM id_status
    WHERE transactions is null
    GROUP BY month
)
SELECT 
    month
    ,p_pageviews / purchaser AS avg_pageviews_purchase
    ,non_p_pageviews / non_purchaser AS avg_pageviews_non_purchase
FROM p_num
JOIN non_p_num
USING (month)
ORDER BY month;
--phần này T đã giải thích chỗ transaction/productRevenue rồi

with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  --and totals.totalTransactionRevenue is not null
  and product.productRevenue is not null
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  and product.productRevenue is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month;


--câu 4 này lưu ý là mình nên dùng left join hoặc full join, bởi vì trong câu này, phạm vi chỉ từ tháng 6-7, nên chắc chắc sẽ có pur và nonpur của cả 2 tháng
--mình inner join thì vô tình nó sẽ ra đúng. nhưng nếu đề bài là 1 khoảng thời gian dài hơn, 2-3 năm chẳng hạn, nó cũng tháng chỉ có nonpur mà k có pur
--thì khi đó inner join nó sẽ làm mình bị mất data, thay vì hiện số của nonpur và pur thì nó để trống



--Cau 5:
WITH id_status AS
(
    SELECT 
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
        ,fullVisitorId AS id
        ,product.productRevenue AS revenue
        ,totals.transactions AS trans 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
        ,UNNEST (hits) AS hits
        ,UNNEST (hits.product) AS product
)
    , p_num AS
(
    SELECT
        month
        ,COUNT(DISTINCT id) AS purchaser
        ,SUM(trans) AS transactions
    FROM id_status
    WHERE revenue is not null
        AND trans is not null 
    GROUP BY month
)
SELECT 
    month
    ,transactions / purchaser AS avg_total_transactions_per_user
FROM p_num;

--mình có thể ghi ngắn gọn lại ntn
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
where  totals.transactions>=1
and totals.totalTransactionRevenue is not null
and product.productRevenue is not null
group by month;

-- Cau 6: KHONG GIONG OUTPUT
WITH info AS
(
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
        ,totals.visits AS visit
        ,product.productRevenue AS revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`   
        ,UNNEST (hits) AS hits
        ,UNNEST (hits.product) AS product
    WHERE totals.transactions is not null 
        AND product.productRevenue is not null 
)
SELECT 
    month
    ,(SUM(revenue) / SUM(visit)) / 1000000 AS avg_revenue_by_user_per_visit 
FROM info
GROUP BY month;

--ghi ngắn gọi lại ntn
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,unnest(hits) hits
  ,unnest(product) product
where product.productRevenue is not null
group by month;


-- Cau 7:

WITH youtubemen_buyer_id AS
(
    SELECT
        fullVisitorId AS id
        ,product.v2ProductName AS product
        ,SUM(product.productRevenue) AS revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`   
        ,UNNEST (hits) AS hits
        ,UNNEST (hits.product) AS product
    WHERE product.productRevenue is not null
        AND product.v2ProductName = "YouTube Men's Vintage Henley"
    GROUP BY id, product
)
    , youtubemen_excluded_products AS
(
    SELECT
        fullVisitorId AS id
        ,product.v2ProductName AS product
        ,SUM(product.productQuantity) AS quantity
        ,SUM(product.productRevenue) AS revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`   
        ,UNNEST (hits) AS hits
        ,UNNEST (hits.product) AS product
    WHERE product.productRevenue is not null
        AND product.v2ProductName <> "YouTube Men's Vintage Henley"
    GROUP BY id, product
)
SELECT
   youtubemen_excluded_products.product AS other_purchased_products
   ,quantity
FROM youtubemen_buyer_id
LEFT JOIN youtubemen_excluded_products
USING (id)
ORDER BY quantity DESC;

--mình có thể dùng left join hoặc subquery theo 2 cách dưới đây

--subquery:
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null)
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc;

--CTE:

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC;


-- Cau 8:
WITH addtocart AS
(
        SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
        ,COUNT(eCommerceAction.action_type) AS num_addtocart
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`   
                ,UNNEST (hits) AS hits
        WHERE _table_suffix BETWEEN '0101' AND '0331'
                AND eCommerceAction.action_type = '3'
        GROUP BY month 
)
    , productview AS
(
        SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
        ,COUNT(eCommerceAction.action_type) AS num_product_view
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`   
                ,UNNEST (hits) AS hits
        WHERE _table_suffix BETWEEN '0101' AND '0331'
                AND eCommerceAction.action_type = '2'
        GROUP BY month 
)
    , id_purchase_revenue AS -- this is the first step to inspect the purchase step
(
                SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
        ,fullVisitorId
        ,eCommerceAction.action_type
        ,product.productRevenue -- notice that not every purchase step that an ID made that the revenue was recorded (maybe refund?).
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`   
                ,UNNEST (hits) AS hits
                ,UNNEST (hits.product) AS product -- productrevenue 
        WHERE _table_suffix BETWEEN '0101' AND '0331'
                AND eCommerceAction.action_type = '6'
)
    , purchase AS   
(
        SELECT 
            month
            ,COUNT(action_type) AS num_purchase  
        FROM id_purchase_revenue 
        WHERE productRevenue IS NOT NULL
        GROUP BY month
)
SELECT 
        month
        ,num_product_view
        ,num_addtocart
        ,num_purchase
        ,ROUND(num_addtocart / num_product_view * 100.0, 2) AS add_to_cart_rate
        ,ROUND(num_purchase / num_product_view * 100.0, 2) AS purchase_rate
FROM productview
JOIN addtocart
USING (month)
JOIN purchase
USING (month)
ORDER BY month;


--bài này count theo action type là sai. vì đề bài yêu cầu mình số lượng sản phẩm. vd như 1 lần purchase, có thể sẽ có nhiều sản phẩm trong đó, nên mình count theo lượt purchase sẽ k phản ảnh đúng được số lượng sp đc purchased

--dùng CTE
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month
join purchase p on pv.month = p.month
order by pv.month;


--Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;


                                                                    ----good----
