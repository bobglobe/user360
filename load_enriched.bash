#!/bin/bash
# BQ Build Incremental Enriched
# bq mk --project_id=sandbox-airflow-test --table --time_partitioning_type=DAY --clustering_fields=user_id --schema=vandenborre/enriched_user.json vandenborre.brute_force_enriched_user
# Parameters:
#BQ_PROJECT
#DATASET
#INPUT DATE
# bash bq_build_past_purchase_array_incremental.bash sandbox-airflow-test bq_structure 2020-03-01
#
BQ_PROJECT=$1
DATASET=$2
USER_TABLE=enriched_user
PURCHASE_TABLE=purchases
OUTPUT_TABLE=enriched_user

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     dateFn=date;;
    Darwin*)    dateFn=gdate;;
    CYGWIN*)    dateFn=date;;
    MINGW*)     dateFn=date;;
    *)          dateFn="UNKNOWN:${unameOut}"
esac

today=$(${dateFn} -d $3 +'%Y-%m-%d')
yesterday=$(${dateFn} -d "$3 -1days" +%Y-%m-%d)


  query="
    WITH  yesterday_enriched_user AS (
    SELECT  user_id,
            DATE('$yesterday') AS dat,
            past_purchases
    FROM    $BQ_PROJECT.$DATASET.$USER_TABLE
    WHERE   date='$yesterday'
  ),
        todays_purchases AS
  (
    SELECT  user_id,
            DATE('$today') AS dat,
            ARRAY_AGG(  STRUCT( date,
                                sku,
                                quantity,
                                purchase_price as price
                              )
                     ) AS today_purchase_array
    FROM  $BQ_PROJECT.$DATASET.purchases
    WHERE date = '$today'
    GROUP BY  user_id
  )

  SELECT  user_id,
          DATE('$today') AS date,
          ARRAY_CONCAT_AGG(purchases) AS past_purchases
  FROM
        ( SELECT  user_id,
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
  bq query --debug_mode -n 0 --replace --use_legacy_sql=false --destination_table $DATASET.${OUTPUT_TABLE}\$$3 "$query"
