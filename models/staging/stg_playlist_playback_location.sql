with playlist_playback_location as (
    select *
    from {{ source('raw_analytics', 'playlist_playback_location') }}
)
select * from playlist_playback_location