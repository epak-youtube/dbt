with channel_basics__video as (
    select
        cb.channel_id,
        cb.video_id,
        v.title as video_title,
        v.published_at_utc,
        dateadd(    -- trick to add a UTC timezone to a timestamp_ntz
                hour,
                -1 * datediff(hour, v.published_at_utc::TIMESTAMP_NTZ, convert_timezone('UTC',v.published_at_utc)::timestamp_ntz),
                convert_timezone('UTC',v.published_at_utc)
                ) as published_at_w_tz_appended,
        convert_timezone('America/Chicago', published_at_w_tz_appended) as video_published_at,
        cb.date as calendar_date,
        cb.live_or_on_demand,
        cb.subscribed_status,
        cb.country_code,
        cb.view_count,
        cb.like_count,
        cb.dislike_count,
        cb.subscriber_gain_count,
        cb.subscribers_lost_count,
        cb.watch_time_in_minutes,
        cb.avg_view_duration_in_seconds
    from {{ ref("stg_channel_basic") }} as cb
    left join {{ ref("stg_video") }} as v on cb.video_id = v.video_id
    where cb.video_id is distinct from 'M8JBkd8KMJA'    -- has no match in the videos table; only has 2 views so just removing
)
select
    *
    exclude(published_at_utc, published_at_w_tz_appended)
from channel_basics__video