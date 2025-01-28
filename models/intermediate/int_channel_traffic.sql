{{
    config(
        materialized = 'incremental',
        full_refresh = false,
        on_schema_change = 'fail'
        )
}}

with channel_traffic as (
    select
        ct.channel_id,
        ct.video_id,
        ct.calendar_date,
        ct.live_or_on_demand,
        ct.subscribed_status,
        ct.traffic_source_id,
        {# documentation on traffic sources: https://developers.google.com/youtube/reporting/v1/reports/dimensions#Traffic_Source_Dimensions #}
        ts.traffic_source_name,
        ct.traffic_source_detail_raw,
        ct.country_code,
        cc.country_name,
        ct.view_count,
        ct.watch_time_in_minutes,
        ct.average_view_duration_in_seconds,
        ct.red_view_count,
        ct.red_watch_time_in_minutes,
        ct._fivetran_synced
    from {{ ref("stg_channel_traffic") }} as ct
    left join {{ ref("country_codes_lookup") }} as cc on cc.country_code = ct.country_code
    left join {{ ref("traffic_source_lookup") }} as ts on ts.traffic_source_id = ct.traffic_source_id
)
select
    *
from channel_traffic as ct
where true
{% if is_incremental() %}
  and ct._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}