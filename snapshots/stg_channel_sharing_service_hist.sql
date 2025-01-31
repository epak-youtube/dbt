{% snapshot stg_channel_sharing_service_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'share_count'
       ],
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel_sharing_service") }}

{% endsnapshot %}