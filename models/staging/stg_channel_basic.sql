with channel_basic as (
    select *
    from {{ source('raw_analytics', 'channel_basic') }}
),
all_data as (
    select
        channel_id,
        nullif(video_id, '') as video_id,
        date as calendar_date,
        initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
        initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
        country_code,
        views as total_view_count,
        likes as like_count,
        dislikes as dislike_count,
        subscribers_gained as subscriber_gain_count,
        subscribers_lost as subscribers_lost_count,
        watch_time_minutes::dec(18, 2) as total_watch_time_in_minutes,
        average_view_duration_seconds::dec(10, 4) as avg_view_duration_in_seconds, -- todo: does this foot?
        card_impressions,
        card_clicks,
        card_teaser_impressions,
        card_teaser_clicks,
        comments as comment_count,
        shares as share_count,
        videos_added_to_playlists,
        videos_removed_from_playlists,
        red_views as red_view_count,
        red_watch_time_minutes::dec(18, 2) as red_watch_time_in_minutes, -- todo: research what "red" watch time means
        _fivetran_synced,
        {{ build_unique_key([
            'video_id',
            'calendar_date',
            'live_or_on_demand',
            'subscribed_status',
            'country_code'
            ]) }} as id
    from channel_basic
)
select
    id,
    channel_id,
    video_id,
    calendar_date,
    live_or_on_demand,
    subscribed_status,
    country_code,
    total_view_count,
    like_count,
    dislike_count,
    subscriber_gain_count,
    subscribers_lost_count,
    total_watch_time_in_minutes,
    avg_view_duration_in_seconds,
    card_impressions,
    card_clicks,
    card_teaser_impressions,
    card_teaser_clicks,
    comment_count,
    share_count,
    videos_added_to_playlists,
    videos_removed_from_playlists,
    red_view_count,
    red_watch_time_in_minutes,
    _fivetran_synced
from all_data