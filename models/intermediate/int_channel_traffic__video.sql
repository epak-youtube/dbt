{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
        )
}}

with channel_traffic__video as (
    select
        ct.*,
        v.video_title,
        v.published_at as video_published_at,
        v.category_name as video_category,
        datediff(day, video_published_at, calendar_date) as days_since_published,
        {# documentation on traffic sources: https://developers.google.com/youtube/reporting/v1/reports/dimensions#Traffic_Source_Dimensions #}
        ts.traffic_source_name,
        coalesce(
            chan_source.title,
            case when ts.traffic_source_name = 'YouTube channels' and chan_source.title is null then 'Other Channel' end,
            v_source.video_title,
            case when ts.traffic_source_name in ('Suggested videos', 'Interactive video endscreen', 'Video cards and annotations') and v_source.video_title is null then 'Other Video' end,
            initcap(replace(ct.traffic_source_raw, '_', ' '))) as traffic_source_detail,
        cc.country_name
    from {{ ref("stg_channel_traffic") }} as ct
    left join {{ ref("int_video") }} as v on ct.video_id = v.video_id
    left join {{ ref("country_codes_lookup") }} as cc on cc.country_code = ct.country_code
    left join {{ ref("traffic_source_lookup") }} as ts on ts.traffic_source_id = ct.traffic_source_id
    left join {{ ref("int_video") }} as v_source on ct.traffic_source_raw = v_source.video_id
    left join {{ ref("stg_channel") }} as chan_source on ct.traffic_source_raw = chan_source.channel_id
    where ct.video_id is distinct from 'M8JBkd8KMJA'    -- has no match in the videos table and only 2 views so just removing
)
select
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
    _fivetran_synced
from channel_traffic__video as ctv
where true
{% if is_incremental() %}
  and ctv._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}