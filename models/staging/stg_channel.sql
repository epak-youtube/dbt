with channel as (
    select *
    from {{ source('raw_analytics', 'channel') }}
)
select
    id as channel_id,
    snippet_title as channel_title,
    snippet_description as channel_description,
    parse_json(snippet_thumbnails) as channel_thumbnails_json,
    statistics_video_count as number_of_videos,
    statistics_view_count as number_of_views,
    statistics_subscriber_count as number_of_subscribers
from channel