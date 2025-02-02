with channel_traffic_sources as (
    select *
    from {{ source('raw_analytics', 'channel_traffic_sources') }}
),
all_data as (
    select
        channel_id,
        nullif(video_id, '') as video_id,
        date as calendar_date,
        initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
        initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
        traffic_source_type as traffic_source_id,
        nullif(traffic_source_detail, '') as traffic_source_detail_raw,
        country_code,
        views as view_count,
        watch_time_minutes::dec(18, 2) as watch_time_in_minutes,
        average_view_duration_seconds::dec(18, 2) as average_view_duration_in_seconds,
        (average_view_duration_percentage / 100)::dec(18, 2) as average_view_duration_percentage,
        red_views as red_view_count,
        red_watch_time_minutes::dec(18, 2) as red_watch_time_in_minutes, -- todo: research what "red" watch time means
        _fivetran_synced,
        {{ build_unique_key([
            'video_id',
            'calendar_date',
            'live_or_on_demand',
            'subscribed_status',
            'country_code',
            'traffic_source_id',
            'traffic_source_detail_raw'
            ]) }} as id
    from channel_traffic_sources
)
select
    id,
    channel_id,
    video_id,
    calendar_date,
    live_or_on_demand,
    subscribed_status,
    traffic_source_id,
    traffic_source_detail_raw,
    country_code,
    view_count,
    watch_time_in_minutes,
    average_view_duration_in_seconds,
    average_view_duration_percentage,
    red_view_count,
    red_watch_time_in_minutes,
    _fivetran_synced
from all_data