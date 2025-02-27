{% snapshot stg_channel_basic_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'like_count',
            'dislike_count',
            'subscriber_gain_count',
            'subscribers_lost_count',
            'total_watch_time_in_minutes',
            'avg_view_duration_in_seconds',
            'card_impressions',
            'card_clicks',
            'card_teaser_impressions',
            'card_teaser_clicks',
            'comment_count',
            'share_count',
            'videos_added_to_playlists',
            'videos_removed_from_playlists',
            'red_view_count',
            'red_watch_time_in_minutes'
       ],
       updated_at = '_fivetran_synced::timestampntz',
       hard_deletes = 'new_record'
   )    
}}

select * from {{ ref("stg_channel_basic") }}

{% endsnapshot %}