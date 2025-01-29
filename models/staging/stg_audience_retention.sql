with audience_retention as (
    select *
    from {{ source('raw_analytics', 'audience_retention') }}
)
select
    video_id,
    date as calendar_date,
    elapsed_video_time_ratio::dec(18, 2) as percent_of_video_elapsed,
    audience_watch_ratio::dec(18, 4) as percent_watch_ratio,
    relative_retention_performance::dec(18, 4) as relative_retention_performance,
    _fivetran_synced,
    {{ build_unique_key([
        'video_id',
        'calendar_date',
        'percent_of_video_elapsed'
        ]) }} as id
from audience_retention