{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['id', 'record_effective_start_timestamp'],
        on_schema_change = 'fail'
        )
}}

with channel_combined as (
    select *
    from {{ ref("int_channel_combined") }}
),
video as (
    select *
    from {{ ref("int_video_current_record") }}
),
all_data as (
    select
        cc.id,
        cc.channel_id,
        cc.video_id,
        v.video_title,
        v.published_at as video_published_at,
        v.category_name as video_category,
        cc.calendar_date,
        cc.country_code,
        cc.country_name,
        cc.live_or_on_demand,
        cc.subscribed_status,
        cc.playback_location_type_id,
        cc.playback_location_type,
        cc.traffic_source_type_id,
        cc.traffic_source_type,
        cc.device_type_id,
        cc.device_type,
        cc.operating_system_id,
        cc.operating_system,
        cc.view_count,
        cc.watch_time_in_minutes,
        cc.average_view_duration_in_seconds,
        cc.red_view_count,
        cc.red_view_watch_time_in_minutes,
        cc.record_effective_start_timestamp,
        cc.record_effective_end_timestamp,
        cc.is_deleted
    from channel_combined as cc
    left join video as v on cc.video_id = v.video_id
)
select
    *
from all_data ad

where true
{% if is_incremental() %}
  and coalesce(ad.record_effective_end_timestamp, current_timestamp) > coalesce((select max(record_effective_start_timestamp) from {{ this }}), '1900-01-01')
{% endif %}