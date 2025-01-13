{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
        )
}}

with video as (
    select *
    from {{ ref("stg_video") }}
),
video_w_thumbnail_url as (
    -- get the highest res image available
    select
        v.*,
        vtj.value:url::varchar(500) as video_thumbnail_url,
        vtj.value:width::int as video_thumbail_resolution_width
    from video as v
        , lateral flatten(input => video_thumbnail_json) as vtj
    qualify row_number() over (partition by v.video_id order by video_thumbail_resolution_width desc) = 1
),
all_data as (
    select
        v.video_id,
        v.published_at,
        v.channel_id,
        v.video_title,
        v.video_description,
        v.category_id,
        cat.category_name,
        v.video_duration_in_seconds,
        (v.video_duration_in_seconds / 60)::dec(18, 4) as video_duration_in_minutes,
        v.has_custom_thumbnail,
        v.video_thumbnail_url,
        v.privacy_status,
        v.view_count,
        v.like_count,
        v.dislike_count,
        v.comment_count,
        v.favorite_count,
        v._fivetran_synced,
    from video_w_thumbnail_url as v
    left join {{ ref("category_id_lookup") }} as cat on v.category_id = cat.category_id
)
select
    *
from all_data ad
where true
{% if is_incremental() %}
  and ad._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}