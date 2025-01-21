with channel_demographics as (
    select *
    from {{ source('raw_analytics', 'channel_demographics') }}
)
select
    channel_id,
    nullif(video_id, '') as video_id,
    date as calendar_date,
    initcap(replace(live_or_on_demand, '_', ' ')) as live_or_on_demand,
    initcap(replace(subscribed_status, '_', ' ')) as subscribed_status,
    country_code,
    initcap(replace(gender, '_', ' ')) as gender,
    case
        when age_group = 'AGE_13_17' then 'A. 13-17'
        when age_group = 'AGE_18_24' then 'B. 18-24'
        when age_group = 'AGE_25_34' then 'C. 25-34'
        when age_group = 'AGE_35_44' then 'D. 35-44'
        when age_group = 'AGE_45_54' then 'E. 45-54'
        when age_group = 'AGE_55_64' then 'F. 55-64'
        when age_group = 'AGE_65_' then 'G. 65+'
        else null
        end as age_group,
    (views_percentage / 100)::dec(18,6) as share_of_views_this_video_day,
    _fivetran_synced
from channel_demographics