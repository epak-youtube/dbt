with playlist_traffic_source as (
    select *
    from {{ source('raw_analytics', 'playlist_traffic_source') }}
)
select * from playlist_traffic_source