with playlist_basic as (
    select *
    from {{ source('raw_analytics', 'playlist_basic') }}
)
select * from playlist_basic