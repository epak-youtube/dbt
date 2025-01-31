{% snapshot stg_channel_subtitles_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'view_count',
            'watch_time_in_minutes',
            'average_view_duration_in_seconds',
            'average_view_duration_percentage',
            'red_view_count',
            'red_watch_time_in_minutes'
       ],
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel_subtitles") }}

{% endsnapshot %}