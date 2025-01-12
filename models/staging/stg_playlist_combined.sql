with playlist_combined as (
    select *
    from {{ source('raw_analytics', 'playlist_combined') }}
)
select * from playlist_combined