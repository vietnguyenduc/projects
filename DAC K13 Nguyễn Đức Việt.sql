
--Cau 1:
SELECT 
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month_extract,
    SUM(totals.visits) AS visits,
    SUM(totals.pageviews) AS pageviews,
    SUM(totals.transactions) AS transactions -- transactions per session
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month_extract;

-- Cau 2:
SELECT 
    trafficSource.source AS page_source,
    SUM(totals.visits) AS total_visits,
    SUM(totals.bounces) AS total_no_of_bounces,
    SUM(totals.bounces)/SUM(totals.visits)*100.0 AS bounce_rate -- no round required
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` -- in July 2017
GROUP BY page_source;

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
    WHERE transactions is not null 
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

-- Cau 6:

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,unnest(hits) hits
  ,unnest(product) product
where product.productRevenue is not null
group by month;


-- Cau 7:
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

-- Cau 8:

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
