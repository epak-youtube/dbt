with channel_basics__video as (
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
        cc.country_name,
        cb.total_view_count,
        cb.like_count,
        cb.dislike_count,
        cb.subscriber_gain_count,
        cb.subscribers_lost_count,
        cb.total_watch_time_in_minutes,
        cb.avg_view_duration_in_seconds,
        cb._fivetran_synced
    from {{ ref("stg_channel_basic") }} as cb
    left join {{ ref("int_video") }} as v on cb.video_id = v.video_id
    left join {{ ref("country_codes_lookup") }} as cc on cc.country_code = cb.country_code
    where cb.video_id is distinct from 'M8JBkd8KMJA'    -- has no match in the videos table and only 2 views so just removing
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
from channel_basics__video