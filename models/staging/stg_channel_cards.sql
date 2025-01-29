with channel_cards as (
    select *
    from {{ source('raw_analytics', 'channel_cards') }}
)
select
    channel_id,
    nullif(video_id, '') as video_id,
    card_id,
    card_type,
    date as calendar_date,
    initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
    initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
    country_code,
    card_impressions,
    card_clicks,
    card_teaser_impressions,
    card_teaser_clicks,
    _fivetran_synced,
    {{ build_unique_key([
        'video_id',
        'card_id',
        'calendar_date',
        'live_or_on_demand',
        'subscribed_status',
        'country_code'
        ]) }} as id
from channel_cards