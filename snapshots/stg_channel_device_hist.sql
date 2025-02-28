{% snapshot stg_channel_device_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'view_count',
            'watch_time_in_minutes',
            'average_view_duration_seconds',
            'average_view_duration_percentage',
            'red_view_count',
            'red_watch_time_in_minutes'
       ],
       updated_at = '_fivetran_synced::timestampntz',
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel_device") }}

{% endsnapshot %}