{{
    config(
        materialized = 'incremental',
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
        cc.channel_id,
        cc.video_id,
        v.video_title,
        v.published_at as video_published_at,
        v.category_name as video_category,
        cc.calendar_date,
        cc.country_code,
        cc.live_or_on_demand,
        cc.subscribed_status,
        cc.playback_location_type_id,
        cc.traffic_source_type_id,
        cc.device_type_id,
        cc.operating_system_id,
        cc.view_count,
        cc.watch_time_in_minutes,
        cc.average_view_duration_in_seconds,
        cc.red_view_count,
        cc.red_view_watch_time_in_minutes,
        cc._fivetran_synced,
    from channel_combined as cc
    left join video as v on cc.video_id = v.video_id
)
select
    *
from all_data ad

where true
{% if is_incremental() %}
  and ad._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}