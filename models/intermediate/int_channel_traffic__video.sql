{{
    config(
        materialized = 'incremental',
        full_refresh = false,
        on_schema_change = 'fail'
        )
}}

with channel_traffic as (
    select *
    from {{ ref("int_channel_traffic") }}
),
video as (
    select *
    from {{ ref("int_video_current_record") }}
),
channel as (
    select *
    from {{ ref("stg_channel_hist") }}
    where record_effective_end_timestamp is null
),
channel_traffic__video as (
    select
        ct.*,
        v.video_title,
        v.published_at as video_published_at,
        v.category_name as video_category,
        datediff(day, video_published_at, calendar_date) as days_since_published,
        coalesce(
            chan_source.title,
            case when ct.traffic_source_name = 'YouTube channels' and chan_source.title is null then 'Other Channel' end,
            v_source.video_title,
            case when ct.traffic_source_name in ('Suggested videos', 'Interactive video endscreen', 'Video cards and annotations') and v_source.video_title is null then 'Other Video' end,
            initcap(replace(ct.traffic_source_detail_raw, '_', ' '))) as traffic_source_detail
    from channel_traffic as ct
    left join video as v on ct.video_id = v.video_id
    left join video as v_source on ct.traffic_source_detail_raw = v_source.video_id
    left join channel as chan_source on ct.traffic_source_detail_raw = chan_source.channel_id
)
select
    id,
    channel_id,
    video_id,
    video_title,
    video_category,
    video_published_at,
    days_since_published,
    calendar_date,
    live_or_on_demand,
    subscribed_status,
    traffic_source_id,
    traffic_source_name,
    traffic_source_detail,
    country_code,
    country_name,
    view_count,
    watch_time_in_minutes,
    average_view_duration_in_seconds,
    red_view_count,
    red_watch_time_in_minutes,
    record_effective_start_timestamp,
    record_effective_end_timestamp,
    is_deleted
from channel_traffic__video as ctv
where true
{% if is_incremental() %}
  and ctv.record_effective_start_timestamp > coalesce((select max(record_effective_start_timestamp) from {{ this }}), '1900-01-01')
{% endif %}