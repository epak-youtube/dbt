with channel_end_screens as (
    select *
    from {{ source('raw_analytics', 'channel_end_screens') }}
)
select 
    channel_id,
    video_id,
    date as calendar_date,
    country_code,
    initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
    initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
    end_screen_element_type as end_screen_element_type_id,
    try_to_number(end_screen_element_id, 38, 0) as end_screen_element_id,
    end_screen_element_clicks,
    end_screen_element_impressions,
    (end_screen_element_click_rate / 100)::dec(18,4) as end_screen_element_click_rate,
    _fivetran_synced,
    {{ build_unique_key([
        'video_id',
        'calendar_date',
        'live_or_on_demand',
        'subscribed_status',
        'country_code',
        'end_screen_element_id'
        ]) }} as id
from channel_end_screens