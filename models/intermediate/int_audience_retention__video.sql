{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
        )
}}

with audience_retention__video as (
    select
        v.channel_id,
        ar.video_id,
        v.video_title,
        v.published_at as video_published_at,
        v.category_name as video_category,
        v.video_thumbnail_url,
        v.video_duration_in_seconds,
        ar.calendar_date,
        ar.percent_of_video_elapsed,
        round(v.video_duration_in_seconds * ar.percent_of_video_elapsed, 2) as video_timestamp_in_seconds,
        ar.percent_watch_ratio,
        ar.relative_retention_performance,
        ar._fivetran_synced
    from {{ ref("stg_audience_retention") }} as ar
    left join {{ ref("int_video") }} as v on ar.video_id = v.video_id
    where ar.video_id is distinct from 'M8JBkd8KMJA'    -- has no match in the videos table and only 2 views so just removing
)
select
    *
from audience_retention__video as arv
where true
{% if is_incremental() %}
  and arv._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}