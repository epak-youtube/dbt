with caption as (
    select *
    from {{ source('raw_analytics', 'caption') }}
)
select
    video_id,
    languages,
    round("START", 3) as start_time_seconds,
    round(duration, 3) as duration_seconds,
    text as caption_text,
    _fivetran_synced,
    {{ build_unique_key([
        'video_id',
        'start_time_seconds'
        ]) }} as id
from caption