with videos as (
    select
        *
    from {{ source('raw_analytics', 'videos') }}
),
-- the purpose of this cte is to do a couple of the more complex transformations; no business logic applied
videos_with_calcs as (
    select
        *,
        -- fields to parse video duration from content_details_duration; helper columns will not be materialized
        regexp_replace(content_details_duration, '[PTS]') video_duration_trimmed,
        zeroifnull(try_to_number(split_part(video_duration_trimmed, 'M', 1))) as video_duration_mins,
        zeroifnull(try_to_number(split_part(video_duration_trimmed, 'M', 2))) as video_duration_secs,
        video_duration_mins * 60  + video_duration_secs as video_duration_in_seconds,
        -- trick to add a UTC timezone to a timestamp_ntz, which i then convert to central time
        -- published_at_utc_no_timezone and published_at_w_tz_appended are just helper columns to make logic easier to follow, but will not be materialized
        -- todo: create an append_timezone user-defined function and refactor this away
        snippet_published_at as published_at_utc_no_timezone,
        dateadd(
                hour,
                -1 * datediff(hour, published_at_utc_no_timezone::timestamp_ntz, convert_timezone('UTC', published_at_utc_no_timezone)::timestamp_ntz),
                convert_timezone('UTC', published_at_utc_no_timezone)
                ) as published_at_w_tz_appended,
        convert_timezone('America/Los_Angeles', published_at_w_tz_appended) as published_at
    from videos
),
all_data as (
    select
        id as video_id,
        published_at,
        snippet_channel_id as channel_id,
        snippet_title as video_title,
        nullif(snippet_description, '') as video_description,
        snippet_category_id as category_id,
        video_duration_in_seconds,
        content_details_has_custom_thumbnail as has_custom_thumbnail,
        parse_json(snippet_thumbnails) as video_thumbnail_json,
        initcap(privacy_status) as privacy_status,
        statistics_view_count as view_count,
        statistics_like_count as like_count,
        statistics_dislike_count as dislike_count,
        statistics_comment_count as comment_count,
        statistics_favorite_count as favorite_count,
        _fivetran_synced
    from videos_with_calcs
)
select
    video_id,
    published_at,
    channel_id,
    video_title,
    video_description,
    category_id,
    video_duration_in_seconds,
    has_custom_thumbnail,
    video_thumbnail_json,
    privacy_status,
    view_count,
    like_count,
    dislike_count,
    comment_count,
    favorite_count,
    _fivetran_synced
from all_data