with channel_traffic__video as (
    select
        ct.*,
        v.title as video_title,
        v.published_at as video_published_at,
        datediff(day, video_published_at, calendar_date) as days_since_published,
        case
            when ct.traffic_source_raw = ct.channel_id then 'Channel'
            when v_source.title is not null then 'Video'
            else initcap(replace(ct.traffic_source_raw, '_', ' '))
            end as traffic_source,
        v_source.title as traffic_source_detail_video_title,
        cc.country_name
    from {{ ref("stg_channel_traffic") }} as ct
    left join {{ ref("stg_video") }} as v on ct.video_id = v.video_id
    left join {{ ref("country_codes_lookup") }} as cc on cc.country_code = ct.country_code
    left join {{ ref("stg_video") }} as v_source on ct.traffic_source_raw = v_source.video_id
    where ct.video_id is distinct from 'M8JBkd8KMJA'    -- has no match in the videos table and only 2 views so just removing
)
select
    channel_id,
    video_id,
    video_title,
    video_published_at,
    days_since_published,
    calendar_date,
    live_or_on_demand,
    subscribed_status,
    traffic_source_id,
    traffic_source,
    traffic_source_detail_video_title,
    country_code,
    country_name,
    view_count,
    watch_time_in_minutes,
    avg_view_duration_in_seconds,
    red_views,
    red_watch_time_in_minutes
from channel_traffic__video



