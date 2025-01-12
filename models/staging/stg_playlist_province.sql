with playlist_province as (
    select *
    from {{ source('raw_analytics', 'playlist_province') }}
)
select * from playlist_province