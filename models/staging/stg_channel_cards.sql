with channel_cards as (
    select *
    from {{ source('raw_analytics', 'channel_cards') }}
)
select
    channel_id,
    nullif(video_id, '') as video_id,
    card_id,
    card_type,
    date,
    live_or_on_demand,
    subscribed_status,
    country_code,
    card_impressions,
    card_clicks,
    card_teaser_impressions,
    card_teaser_clicks,
from channel_cards