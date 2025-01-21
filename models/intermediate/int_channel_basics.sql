{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
        )
}}

with channel_basics as (
    select
        cb.channel_id,
        cb.video_id,
        cb.calendar_date,
        cb.live_or_on_demand,
        cb.subscribed_status,
        cb.country_code,
        cc.country_name,
        cb.total_view_count,
        cb.like_count,
        cb.dislike_count,
        cb.subscriber_gain_count,
        cb.subscribers_lost_count,
        cb.total_watch_time_in_minutes,
        cb.avg_view_duration_in_seconds,
        cb.card_impressions,
        cb.card_clicks,
        cb.card_teaser_impressions,
        cb.card_teaser_clicks,
        cb.comment_count,
        cb.share_count,
        cb.videos_added_to_playlists,
        cb.videos_removed_from_playlists,
        cb.red_view_count,
        cb._fivetran_synced
    from {{ ref("stg_channel_basic") }} as cb
    left join {{ ref("country_codes_lookup") }} as cc on cc.country_code = cb.country_code
    where cb.video_id is distinct from 'M8JBkd8KMJA'    -- has no match in the videos table and only 2 views so just removing
)
select
    *
from channel_basics cb
where true
{% if is_incremental() %}
  and cb._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}