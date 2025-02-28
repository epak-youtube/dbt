{% snapshot stg_channel_cards_hist %}

{{
   config(
       database = 'DEV',
       unique_key = 'id',
       strategy = 'check',
       check_cols = [
            'card_type',
            'card_impressions',
            'card_clicks',
            'card_teaser_impressions',
            'card_teaser_clicks'
       ],
       updated_at = '_fivetran_synced::timestampntz',
       hard_deletes = 'new_record'
   )
}}

select * from {{ ref("stg_channel_cards") }}

{% endsnapshot %}