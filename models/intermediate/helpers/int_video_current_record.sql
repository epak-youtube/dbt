{# we need to grab the current video record in a bunch of downstream models, so this model was created to reduce code duplication #}

{{
    config(
        materialized = 'ephemeral'
    )
}}

select *
from {{ ref("int_video") }}
where record_effective_end_timestamp is null