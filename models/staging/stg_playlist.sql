with playlist as (
    select *
    from {{ source('raw_analytics', 'playlist') }}
)
select * from playlist