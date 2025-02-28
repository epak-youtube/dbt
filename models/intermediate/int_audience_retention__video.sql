{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['id', 'record_effective_start_timestamp'],
        on_schema_change = 'fail'
        )
}}

with audience_retention as (
    select *
    from {{ ref("stg_audience_retention_hist") }}
),
video as (
    select *
    from {{ ref("int_video_current_record") }}
),
audience_retention__video as (
    select
        ar.id,
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
        ar.record_effective_start_timestamp,
        ar.record_effective_end_timestamp,
        ar.is_deleted
    from audience_retention as ar
    left join video as v on ar.video_id = v.video_id
    where ar.video_id is distinct from 'M8JBkd8KMJA'    -- has no match in the videos table and only 2 views so just removing
)
select
    *
from audience_retention__video as arv
where true
{% if is_incremental() %}
  and coalesce(arv.record_effective_end_timestamp, current_timestamp) > coalesce((select max(record_effective_start_timestamp) from {{ this }}), '1900-01-01')
{% endif %}