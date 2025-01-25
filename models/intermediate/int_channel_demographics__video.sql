{{
    config(
        materialized = 'incremental',
        full_refresh=false,
        on_schema_change = 'fail'
        )
}}

with channel_demographics as (
    select *
    from {{ ref("int_channel_demographics") }}
),
video as (
    select *
    from {{ ref("int_video_current_record") }}
),
channel_basics as (
    select *
    from {{ ref("int_channel_basics") }}
),
views_by_video_day as (
    select
        video_id,
        calendar_date,
        sum(total_view_count) as total_view_count
    from channel_basics
    group by all
),
all_data as (
    select
        cd.channel_id,
        cd.video_id,
        cd.calendar_date,
        cd.live_or_on_demand,
        cd.subscribed_status,
        cd.country_code,
        cd.country_name,
        cd.gender,
        cd.age_group,
        cd.share_of_views_this_video_day,
        vvd.total_view_count as total_view_count_this_video_day,
        {#
        This field attempts to translate view share into a count by multiplying by the total number of views for that video that day
        However, treat this as an estimate because the sources don't align perfectly; for example, channel_basics says a video had 21 views, but view shares are divisible by 5% (inferring 20 views)
        #}
        (cd.share_of_views_this_video_day * total_view_count_this_video_day)::dec(18, 2) as view_count_estimated,
        cd._fivetran_synced
    from channel_demographics as cd
    left join views_by_video_day as vvd on cd.video_id = vvd.video_id
        and cd.calendar_date = vvd.calendar_date
)
select *
from all_data as ad
where true

{% if is_incremental() %}
  and ad._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %}