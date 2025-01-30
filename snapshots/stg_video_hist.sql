{% snapshot stg_video_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'video_id',
       strategy = 'check',
       check_cols = [
            'video_title',
            'video_description',
            'category_id',
            'video_duration_in_seconds',
            'has_custom_thumbnail',
            'video_thumbnail_json',
            'privacy_status',
            'view_count',
            'like_count',
            'dislike_count',
            'comment_count',
            'favorite_count'
       ],
       hard_deletes = 'new_record'
   )    
}}

select * from {{ ref("stg_video") }}

{% endsnapshot %}