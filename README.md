# user360
## Goal

Create an “enriched_purchase” table with the following schema so that we can train vectorize & train without doing expensive joins

Cf `enriched_purchases.json`


## Input
Purchase table (cf `purchase.json`)
1 file / day




## Current approach and its limit

Asset is build on the fly with very expensive joins going back 24 months of purchase data


## New potential approach

Build incrementally an enriched_user asset that will include in an array all past purchases until today
That way, this enriched_purchase$20200305 can be easily created on the fly by joining the last partition of purchases$20200305 with enriched_user$20200305....With clustering added. This could be a much cheaper approach


## current query (not working)

```
#standardSQL
WITH
  purchases_array_0302 AS (
  SELECT
    user_id,
    DATE('2020-03-02') AS dat,
    ARRAY_AGG(STRUCT(sku,
        date,
        quantity,
        purchase_price ) ) AS past_purchases
  FROM
    `sandbox-airflow-test.bq_structure.purchases`
  WHERE
    (date = '2020-03-01'
      OR date='2020-03-02')
  GROUP BY
    user_id
  ORDER BY
    dat ASC ),
  todays_purchases AS (
  SELECT
    user_id,
    DATE('2020-03-03') AS dat,
    ARRAY_AGG(STRUCT(sku,
        date,
        quantity,
        purchase_price ) ) AS today_purchase_array
  FROM
    `sandbox-airflow-test.bq_structure.purchases`
  WHERE
    date = '2020-03-03'
  GROUP BY
    user_id
  ORDER BY
    dat ASC )
SELECT
  enriched.user_id,
  DATE('2020-03-03') AS dat,
  ARRAY_CONCAT_AGG((
    SELECT
      enriched.past_purchases
    UNION ALL
    SELECT
      today_purchase_array
     FROM todays_purchases))
FROM
  todays_purchases AS today
INNER JOIN
  purchases_array_0302 AS enriched
ON
  today.user_id=enriched.user_id
  GROUP By user_id
```

Error is
`Correlated subqueries that reference other tables are not supported unless they can be de-correlated, such as by transforming them into an efficient JOIN.`
