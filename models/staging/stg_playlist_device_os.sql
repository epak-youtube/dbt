with playlist_device_os as (
    select *
    from {{ source('raw_analytics', 'playlist_device_os') }}
)
select * from playlist_device_os