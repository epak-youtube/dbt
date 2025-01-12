with channel_demographics as (
    select *
    from {{ source('raw_analytics', 'channel_demographics') }}
)
select
    channel_id,
    nullif(video_id, '') as video_id,
    date as calendar_date,
    initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
    initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
    country_code,
    gender,
    age_group,
    views_percentage as video_day_view_percentage,
    _fivetran_synced
from channel_demographics