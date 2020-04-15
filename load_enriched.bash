#!/bin/bash
# BQ Build Incremental Enriched

BQ_PROJECT=sandbox-airflow-test
DATASET=bq_structure2
USER_TABLE=enriched_user_windowed
PURCHASE_TABLE=purchases
OUTPUT_TABLE=enriched_user_windowed
history=3 # in Days

#Step1 : Create the Asset with the folloowing command
#bq mk --project_id=$BQ_PROJECT --table --time_partitioning_type=DAY --time_partitioning_field=date --clustering_fields=user_id --schema=enriched_user.json $DATASET.$OUTPUT_TABLE

#enriched_user_windowed will keep array of past_purchases (i.e. date < today) and date > today -$history.days

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     dateFn=date;;
    Darwin*)    dateFn=gdate;;
    CYGWIN*)    dateFn=date;;
    MINGW*)     dateFn=date;;
    *)          dateFn="UNKNOWN:${unameOut}"
esac

today=$(${dateFn} -d $1 +'%Y-%m-%d')
yesterday=$(${dateFn} -d "$1 -1days" +%Y-%m-%d)
start_date=$(${dateFn} -d "$1 -$history days" +%Y-%m-%d)

  query="
    WITH  yesterday_enriched_user AS (
    SELECT  user_id,
            DATE('$yesterday') AS dat,
            ARRAY_AGG(STRUCT( past.purchase_date,sku, quantity,price)) as past_purchases
    FROM    $BQ_PROJECT.$DATASET.$USER_TABLE, UNNEST(past_purchases) as past
    WHERE   date='$yesterday' and past.purchase_date>='$start_date'
    GROUP BY 1,2
  ),
        todays_purchases AS
  (
    SELECT  user_id,
            DATE('$yesterday') AS dat,
            ARRAY_AGG(  STRUCT( date,
                                sku,
                                quantity,
                                purchase_price as price
                              )
                     ) AS today_purchase_array
    FROM  $BQ_PROJECT.$DATASET.purchases
    WHERE date = '$yesterday'
    GROUP BY  user_id
  )

  SELECT  user_id,
          DATE('$today') AS date,
          ARRAY_CONCAT_AGG(purchases) AS past_purchases
  FROM
        (
          SELECT  user_id,
                  past_purchases AS purchases
          FROM    yesterday_enriched_user AS enriched
          WHERE   dat = '$yesterday'
          UNION ALL
          SELECT  user_id,
                  today_purchase_array as purchases
          FROM    todays_purchases AS today
        ) AS today_agg
  GROUP BY user_id
  "
  echo "$query"
  echo "+++++++++++++++++"
  bq query  -n 0 --replace --use_legacy_sql=false --destination_table $DATASET.${OUTPUT_TABLE}\$$1 "$query"
