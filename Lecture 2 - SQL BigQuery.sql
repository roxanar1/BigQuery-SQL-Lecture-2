
--1) CTEs (WITH) to structure logic--
--What it does: creates named “mini tables” inside a query.--
--Total users + new users--

WITH UserInfo AS (
SELECT
user_pseudo_id,
MAX(IF(event_name IN ('first_visit', 'first_open'), 1, 0)) AS is_new_user
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20201130'
GROUP BY user_pseudo_id
)
SELECT
COUNT(*) AS total_users,
SUM(is_new_user) AS new_users
FROM UserInfo;

--2) Arrays + UNNEST (the GA4 superpower)--

--GA4 stores lots of fields inside arrays:--
--event_params (key/value pairs)--
--items (products in commerce events)--
--Two common patterns:--

--Pattern A — Scalar subquery extraction (simple, safe)--
--Pull one parameter from event_params without exploding rows.--

SELECT
TIMESTAMP_MICROS(event_timestamp) AS event_time,
(
SELECT value.string_value
FROM UNNEST(event_params)
WHERE key = 'page_location' LIMIT 1
) AS page_location
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name = 'page_view'
AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201202'
LIMIT 50;

--Pattern B — Flattening (UNNEST in FROM) for item-level analysis--
--This multiplies rows (one event can have many items).--

SELECT
event_date,
item.item_name,
COUNT(*) AS item_rows
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e,
UNNEST(e.items) AS item
WHERE e.event_name = 'purchase'
AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
GROUP BY event_date, item.item_name
ORDER BY item_rows DESC
LIMIT 20;
--Note: UNNEST changes the “grain.” After unnesting items, you’re no longer at “event-level”; you’re at “item-row per event.”--

--3) STRING_AGG, ARRAY_AGG (useful aggregations)--
--What they do: combine many values into one row per group.--
--Example: show a few event_names seen per day--

SELECT
event_date,
STRING_AGG(DISTINCT event_name, ', ' ORDER BY event_name) AS events_seen
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201203'
GROUP BY event_date
ORDER BY event_date;

--Example: Build a “session cart summary” (top items a user added to cart)--
--Use Case 1:--
-- Task: Create a list of items per session

SELECT
    user_pseudo_id,
    ARRAY_AGG(item_name) AS items_added_to_cart
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
UNNEST(items) AS item
WHERE event_name = 'add_to_cart'
AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
GROUP BY user_pseudo_id
ORDER BY user_pseudo_id
LIMIT 10;

--Use Case 2:--
-- Session-level "cart summary" using ARRAY_AGG

WITH add_to_cart AS (
  SELECT
    user_pseudo_id,
    (
      SELECT value.int_value 
      FROM UNNEST(event_params) 
      WHERE key = 'ga_session_id'
    ) AS session_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    i.item_id,
    i.item_name,
    i.quantity,
    i.price
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS i
  WHERE event_name = 'add_to_cart'
    AND _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'  -- cost control
)
SELECT
  user_pseudo_id,
  session_id,
  COUNT(*) AS total_add_to_cart_events,              -- added insight
  ARRAY_AGG(
    STRUCT(item_id, item_name, quantity, price, event_timestamp)
    ORDER BY quantity DESC, event_timestamp ASC
    LIMIT 10
  ) AS cart_items
FROM add_to_cart
WHERE session_id IS NOT NULL                          --  filter nulls
GROUP BY user_pseudo_id, session_id;

--4) Joins (start with INNER and LEFT)--
--What it does: combines results based on matching keys.--
--Example: daily users joined with daily purchases (using CTEs)--

WITH daily_users AS (
  SELECT
    event_date,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
  GROUP BY event_date
),
daily_purchases AS (
  SELECT
    event_date,
    COUNT(DISTINCT
      (SELECT value.string_value
       FROM UNNEST(event_params)
       WHERE key = 'transaction_id')
    ) AS purchases    -- distinct transactions
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name = 'purchase'
    AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
  GROUP BY event_date
)
SELECT
  u.event_date,
  u.users,
  -- turns “no purchase row (NULL)” into 0 purchases, which is what you want for a daily time series
  IFNULL(p.purchases, 0) AS purchases,              
  -- Conversion rate: what % of daily users made a purchase
  ROUND(
    IFNULL(p.purchases, 0) / NULLIF(u.users, 0) * 100, 2
  ) AS conversion_rate_pct        -- NULLIF prevents division by zero
FROM daily_users u
LEFT JOIN daily_purchases p
  ON u.event_date = p.event_date
ORDER BY u.event_date;

--5) Window functions + QUALIFY--
--What they do: calculate “across rows” without collapsing to one row per group.--
--Top 3 event types per day (RANK) + QUALIFY--

WITH daily_event_counts AS (
SELECT
event_date,
event_name,
COUNT(*) AS events
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201207'
GROUP BY event_date, event_name
)
SELECT
event_date,
event_name,
events,
RANK() OVER (PARTITION BY event_date ORDER BY events DESC) AS rnk
FROM daily_event_counts
QUALIFY rnk <= 3 
ORDER BY event_date, rnk;

--Rolling 7-day average of daily purchases--

WITH daily_purchases AS (
SELECT
PARSE_DATE('%Y%m%d', event_date) AS dt,
COUNT(*) AS purchases
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name = 'purchase'
AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201231' GROUP BY dt
)
SELECT
dt,
purchases,
AVG(purchases) OVER (
ORDER BY dt
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS purchases_7d_avg
FROM daily_purchases
ORDER BY dt;


--6) Approximate functions (performance-minded)--
--What they do: return “close enough” answers faster on huge datasets.--
--Approx distinct users per event type--

SELECT
event_name,
APPROX_COUNT_DISTINCT(user_pseudo_id) AS approx_users
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
GROUP BY event_name
ORDER BY approx_users DESC
LIMIT 15;
