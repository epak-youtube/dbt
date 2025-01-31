with channel_province as (
    select *
    from {{ source('raw_analytics', 'channel_province') }}
),
all_data as (
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
        (average_view_duration_percentage / 100)::dec(18, 4) as average_view_duration_percentage,
        _fivetran_synced,
        {{ build_unique_key([
            'video_id',
            'calendar_date',
            'live_or_on_demand',
            'subscribed_status',
            'country_code',
            'province_code'
            ]) }} as id
    from channel_province
)
select
    id,
    channel_id,
    video_id,
    calendar_date,
    country_code,
    live_or_on_demand,
    subscribed_status,
    province_code,
    view_count,
    watch_time_in_minutes,
    average_view_duration_in_seconds,
    average_view_duration_percentage,
    _fivetran_synced
from all_data