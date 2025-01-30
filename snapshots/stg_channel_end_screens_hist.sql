{% snapshot stg_channel_end_screens_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'end_screen_element_type_id',
            'end_screen_element_clicks',
            'end_screen_element_impressions',
            'end_screen_element_click_rate'
       ],
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel_end_screens") }}

{% endsnapshot %}