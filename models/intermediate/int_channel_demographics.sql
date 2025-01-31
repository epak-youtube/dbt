{{
    config(
        materialized = 'incremental',
        full_refresh = false,
        on_schema_change = 'fail'
        )
}}

with channel_demographics as (
    select *
    from {{ ref("stg_channel_demographics_hist") }}
),
country_code as (
    select *
    from {{ ref("country_codes_lookup") }}
),
all_data as (
    select
        cd.id,
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
        cd.record_effective_start_timestamp,
        cd.record_effective_end_timestamp,
        cd.is_deleted
    from channel_demographics as cd
    left join country_code as cc on cd.country_code = cc.country_code
)
select *
from all_data as ad
where true

{% if is_incremental() %}
  and ad.record_effective_start_timestamp > coalesce((select max(record_effective_start_timestamp) from {{ this }}), '1900-01-01')
{% endif %}