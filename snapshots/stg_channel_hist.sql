{% snapshot stg_channel_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'channel_id',
       strategy = 'check',
       check_cols = [
            'title',
            'custom_url',
            'description',
            'thumbnails_json',
            'number_of_views',
            'number_of_subscribers',
            'number_of_videos',
            'topic_ids'
       ],
       updated_at = '_fivetran_synced::timestampntz',
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel") }}

{% endsnapshot %}