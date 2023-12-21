{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    tags = [ "top-level-intermediate" ]
) }}

select {{
    dbt_utils.surrogate_key([
    'return_items', 'user_email', 'user_portrait', 'username', 'image_return', 'reason', 'status', 'activity',
    'currency', 'due_date', 'order_sn', 'return_sn', 'create_time', 'negotiation_counter_limit', 'negotiation_offer_due_date',
    'negotiation_latest_solution', 'negotiation_status', 'negotiation_latest_offer_amount', 'negotiation_latest_offer_creator',
    'text_reason', 'update_time', 'seller_proof_status', 'seller_evidence_deadline', 'refund_amount', 'needs_logistics',
    'tracking_number', 'logistics_status', 'seller_compensation_amount', 'seller_compensation_status',
    'seller_compensation_due_date', 'return_ship_due_date', 'return_pickup_city', 'return_pickup_name', 'return_pickup_town',
    'return_pickup_phone', 'return_pickup_state', 'return_pickup_region', 'return_pickup_address', 'return_pickup_zipcode',
    'return_pickup_district', 'amount_before_discount', 'return_seller_due_date', '_AIRBYTE_AB_ID', '_AIRBYTE_EMITTED_AT',
    '_AIRBYTE_NORMALIZED_AT'
])
}} as _AIRBYTE_RETURN_HASHID,
    tmp.*
from {{ref('return')}} tmp

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}