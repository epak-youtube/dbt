{{
    config(
        materialized = 'incremental',
        full_refresh = false,
        on_schema_change = 'fail'
        )
}}

with channel_basics as (
    select *
    from {{ ref("int_channel_basics") }}
),
video as (
    select *
    from {{ ref("int_video_current_record") }}
),
channel_basics__video as (
    select
        cb.id,
        cb.channel_id,
        cb.video_id,
        v.video_title,
        v.published_at as video_published_at,
        v.category_name as video_category,
        cb.calendar_date,
        datediff(day, video_published_at, calendar_date) as days_since_published,
        cb.live_or_on_demand,
        cb.subscribed_status,
        cb.country_code,
        cb.country_name,
        cb.total_view_count,
        cb.like_count,
        cb.dislike_count,
        cb.subscriber_gain_count,
        cb.subscribers_lost_count,
        cb.total_watch_time_in_minutes,
        cb.avg_view_duration_in_seconds,
        cb.record_effective_start_timestamp,
        cb.record_effective_end_timestamp,
        cb.is_deleted
    from channel_basics as cb
    left join video as v on cb.video_id = v.video_id
)
select
    *
from channel_basics__video as cbv
where true
{% if is_incremental() %}
  and cbv.record_effective_start_timestamp > coalesce((select max(record_effective_start_timestamp) from {{ this }}), '1900-01-01')
{% endif %}