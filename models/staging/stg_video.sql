with videos as (
    select *
    from raw.youtube_analytics.video
)
select
    id as video_id,
    snippet_published_at as published_at_utc,
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
from videos