{% snapshot stg_channel_demographics_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'share_of_views_this_video_day'
       ],
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel_demographics") }}

{% endsnapshot %}