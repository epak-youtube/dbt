with channel as (
    select *
    from {{ source('raw_analytics', 'channel') }}
),
all_data as (
    select
        id as channel_id,
        snippet_title as title,
        snippet_custom_url as custom_url,
        snippet_description as description,
        parse_json(snippet_thumbnails) as thumbnails_json,
        statistics_view_count as number_of_views,
        statistics_subscriber_count as number_of_subscribers,
        statistics_video_count as number_of_videos,
        parse_json(topic_details):topicIds as topic_ids,
        _fivetran_synced
    from channel
)
select
    channel_id,
    title,
    custom_url,
    description,
    thumbnails_json,
    number_of_views,
    number_of_subscribers,
    number_of_videos,
    topic_ids,
    _fivetran_synced
from all_data