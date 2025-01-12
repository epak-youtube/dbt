with channel_province as (
    select *
    from {{ source('raw_analytics', 'channel_province') }}
)
select
    channel_id,
    video_id,
    date as calendar_date,
    country_code,
    initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
    initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
    province_code,
    views as view_count,
    watch_time_minutes as watch_time_in_minutes,
    average_view_duration_seconds::dec(18, 2) as average_view_duration_in_seconds,
    average_view_duration_percentage::dec(18, 2) as average_view_duration_percentage,
    _fivetran_synced
from channel_province