with channel_basic as (
    select *
    from raw.youtube_analytics.channel_basic_a_2
)
select
    channel_id,
    nullif(video_id, '') as video_id,
    date,
    live_or_on_demand,
    subscribed_status,
    country_code,
    views as view_count,
    likes as like_count,
    dislikes as dislike_count,
    subscribers_gained as subscriber_gain_count,
    subscribers_lost as subscribers_lost_count,
    watch_time_minutes as watch_time_in_minutes,
    average_view_duration_seconds as avg_view_duration_in_secs, -- does this foot?
    card_impressions,
    card_clicks,
    card_teaser_impressions,
    card_teaser_clicks,
    comments as comment_count,
    shares as share_count,
    videos_added_to_playlists,
    videos_removed_from_playlists,
    red_views,
    red_watch_time_minutes
from channel_basic