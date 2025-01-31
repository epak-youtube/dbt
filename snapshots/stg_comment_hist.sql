{% snapshot stg_comment_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'comment_id',
       strategy = 'timestamp',
       updated_at = 'updated_at',
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_comment") }}

{% endsnapshot %}