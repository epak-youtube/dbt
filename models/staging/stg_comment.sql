with comment as (
    select *
    from {{ source('raw_analytics', 'comment') }}
)
select
    id as comment_id,
    convert_timezone('America/Chicago', snippet_published_at) as comment_published_at,
    convert_timezone('America/Chicago', snippet_updated_at) as updated_at,
    video_id,
    snippet_text_display as comment_text,
    is_public,
    snippet_author_display_name as commenter_display_name,
    snippet_author_channel_url as commenter_channel_url,
    snippet_parent_id as parent_comment_id,
    snippet_moderation_status as moderation_status,
    snippet_like_count as count_likes,
    total_reply_count as count_replies,
    _fivetran_synced
from comment