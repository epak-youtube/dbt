{% snapshot stg_audience_retention_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'percent_watch_ratio',
            'relative_retention_performance'
       ],
       updated_at = '_fivetran_synced::timestampntz',
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_audience_retention") }}

{% endsnapshot %}