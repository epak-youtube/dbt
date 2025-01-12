with audience_retention as (
    select *
    from {{ source('raw_analytics', 'audience_retention') }}
)
select
    video_id,
    date as calendar_date,
    elapsed_video_time_ratio as percent_of_video_elapsed,
    round(audience_watch_ratio, 4) as percent_watch_ratio,
    relative_retention_performance,
    _fivetran_synced
from audience_retention