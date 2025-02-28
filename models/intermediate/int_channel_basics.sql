{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['id', 'record_effective_start_timestamp'],
        on_schema_change = 'fail'
        )
}}

with channel_basics as (
    select
        cb.id,
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
        cb.record_effective_start_timestamp,
        cb.record_effective_end_timestamp,
        cb.is_deleted
    from {{ ref("stg_channel_basic_hist") }} as cb
    left join {{ ref("country_codes_lookup") }} as cc on cc.country_code = cb.country_code
)
select
    *
from channel_basics cb
where true
{% if is_incremental() %}
  and coalesce(cb.record_effective_end_timestamp, current_timestamp) > coalesce((select max(record_effective_start_timestamp) from {{ this }}), '1900-01-01')
{% endif %}