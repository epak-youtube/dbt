{{
    config(
        materialized = 'incremental',
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
        cb._fivetran_synced
    from channel_basics as cb
    left join video as v on cb.video_id = v.video_id
)
select
    channel_id,
    video_id,
    video_title,
    video_published_at,
    video_category,
    calendar_date,
    days_since_published,
    live_or_on_demand,
    subscribed_status,
    country_code,
    country_name,
    total_view_count,
    like_count,
    dislike_count,
    subscriber_gain_count,
    subscribers_lost_count,
    total_watch_time_in_minutes,
    avg_view_duration_in_seconds,
    _fivetran_synced
from channel_basics__video as cbv
where true
{% if is_incremental() %}
  and cbv._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}