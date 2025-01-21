{# we need to grab the current video record in a bunch of downstream models, so this model was created to reduce code duplication #}

{{
    config(
        materialized = 'ephemeral'
    )
}}

select *
from {{ ref("int_video") }}
qualify row_number() over (partition by video_id order by _fivetran_synced desc) = 1