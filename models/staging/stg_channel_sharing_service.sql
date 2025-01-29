with channel_sharing_service as (
    select *
    from {{ source('raw_analytics', 'channel_sharing_service') }}
)
select
    channel_id,
    video_id,
    date as calendar_date,
    sharing_service as sharing_service_id,
    country_code,
    initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
    initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
    shares as share_count,
    _fivetran_synced,
    {{ build_unique_key([
        'video_id',
        'calendar_date',
        'live_or_on_demand',
        'subscribed_status',
        'country_code',
        'sharing_service_id'
        ]) }} as id
from channel_sharing_service