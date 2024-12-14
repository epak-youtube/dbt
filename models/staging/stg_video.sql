with videos as (
    select *
    from {{ source('raw_analytics', 'videos') }}
),
append_timezone as (
    select *,
        -- trick to add a UTC timezone to a timestamp_ntz, which i then convert to central time
        -- published_at_utc_no_timezone and published_at_w_tz_appended are just helper columns to make logic easier to follow, but will not be materialized
        snippet_published_at as published_at_utc_no_timezone,
        dateadd(
                hour,
                -1 * datediff(hour, published_at_utc_no_timezone::timestamp_ntz, convert_timezone('UTC', published_at_utc_no_timezone)::timestamp_ntz),
                convert_timezone('UTC', published_at_utc_no_timezone)
                ) as published_at_w_tz_appended,
        convert_timezone('America/Chicago', published_at_w_tz_appended) as published_at
    from videos
)
select
    id as video_id,
    published_at,
    snippet_channel_id as channel_id,
    snippet_title as title,
    content_details_has_custom_thumbnail as has_custom_thumbnail,
    parse_json(snippet_thumbnails) as video_thumbnail_json,
    initcap(privacy_status) as privacy_status,
    statistics_view_count as view_count,
    statistics_like_count as like_count,
    statistics_dislike_count as dislike_count,
    statistics_comment_count as comment_count,
    statistics_favorite_count as favorite_count,
from append_timezone