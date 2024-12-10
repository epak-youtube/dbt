with channel_demographics as (
    select *
    from raw.youtube_analytics.channel_demographics_a_1
)
select
    channel_id,
    nullif(video_id, '') as video_id,
    date,
    live_or_on_demand,
    subscribed_status,
    country_code,
    gender,
    age_group,
    views_percentage as video_day_view_percentage
from channel_demographics