{{
    config(
        materialized = 'incremental',
        full_refresh = false,
        on_schema_change = 'fail'
        )
}}

with channel_demographics as (
    select *
    from {{ ref("stg_channel_demographics") }}
),
country_code as (
    select *
    from {{ ref("country_codes_lookup") }}
),
all_data as (
    select
        cd.channel_id,
        cd.video_id,
        cd.calendar_date,
        cd.live_or_on_demand,
        cd.subscribed_status,
        cd.country_code,
        cc.country_name,
        cd.gender,
        cd.age_group,
        cd.share_of_views_this_video_day,
        cd._fivetran_synced
    from channel_demographics as cd
    left join country_code as cc on cd.country_code = cc.country_code
)
select *
from all_data as ad
where true

{% if is_incremental() %}
  and ad._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}