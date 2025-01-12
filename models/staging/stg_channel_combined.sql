with channel_combined as (
    select *
    from {{ source('raw_analytics', 'channel_combined') }}
)
select
    channel_id,
    video_id,
    date as calendar_date,
    country_code,
    initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
    initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
    playback_location_type as playback_location_type_id,
    traffic_source_type as traffic_source_type_id,
    device_type as device_type_id,
    operating_system as operating_system_id,
    views as view_count,
    watch_time_minutes::dec(18, 4) as watch_time_in_minutes,
    average_view_duration_seconds::dec(18, 4) as average_view_duration_in_seconds,
    average_view_duration_percentage::dec(18, 4) as average_view_duration_percentage,
    red_views as red_view_count,
    red_watch_time_minutes::dec(18, 4) as red_view_watch_time_in_minutes,
    _fivetran_synced
from channel_combined