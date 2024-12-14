with channel_demographics as (
    select *
    from {{ source('raw_analytics', 'channel_demographics') }}
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