{% snapshot stg_channel_sharing_service_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'share_count'
       ],
       updated_at = '_fivetran_synced::timestampntz',
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel_sharing_service") }}

{% endsnapshot %}