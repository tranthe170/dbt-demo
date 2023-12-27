{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    tags = [ "top-level-intermediate" ]
) }}


select
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'item')::json as return_items,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'user' ->> 'email')::text AS user_email,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'user' ->> 'portrait')::text AS user_portrait,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'user' ->> 'username')::text AS username,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'image')::json as image_return,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'reason')::text AS reason,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'status')::text AS status,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'activity')::json AS activity,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'currency')::text AS currency,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'due_date')::numeric) AT TIME ZONE 'UTC' AS due_date,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'order_sn')::text AS order_sn,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'return_sn')::text AS return_sn,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'create_time')::numeric) AT TIME ZONE 'UTC' AS create_time,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'negotiation'->> 'counter_limit')::text AS negotiation_counter_limit,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'negotiation'->> 'offer_due_date')::numeric) AT TIME ZONE 'UTC' AS negotiation_offer_due_date,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'negotiation'->> 'latest_solution')::text AS negotiation_latest_solution,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'negotiation'->> 'negotiation_status')::text AS negotiation_status,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'negotiation'->> 'latest_offer_amount')::numeric AS negotiation_latest_offer_amount,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'negotiation'->> 'latest_offer_creator')::text AS negotiation_latest_offer_creator,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'text_reason')::text AS text_reason,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'update_time')::numeric) AT TIME ZONE 'UTC' AS update_time,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'seller_proof'->> 'seller_proof_status')::text AS seller_proof_status,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'seller_proof'->> 'seller_evidence_deadline')::numeric) AT TIME ZONE 'UTC' AS seller_evidence_deadline,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'refund_amount')::numeric AS refund_amount,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'needs_logistics')::boolean AS needs_logistics,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'tracking_number')::text AS tracking_number,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'logistics_status')::text AS logistics_status,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'seller_compensation'->> 'compensation_amount')::numeric AS seller_compensation_amount,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'seller_compensation'->> 'seller_compensation_status')::text AS seller_compensation_status,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'seller_compensation'->> 'seller_compensation_due_date')::numeric) AT TIME ZONE 'UTC' AS seller_compensation_due_date,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'return_ship_due_date')::numeric) AT TIME ZONE 'UTC' AS return_ship_due_date,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'city')::text AS return_pickup_city,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'name')::text AS return_pickup_name,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'town')::text AS return_pickup_town,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'phone')::text AS return_pickup_phone,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'state')::text AS return_pickup_state,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'region')::text AS return_pickup_region,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'address')::text AS return_pickup_address,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'zipcode')::text AS return_pickup_zipcode,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' -> 'return_pickup_address'->> 'district')::text AS return_pickup_district,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'amount_before_discount')::numeric AS amount_before_discount,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'detail' ->> 'return_seller_due_date')::numeric) AT TIME ZONE 'UTC' AS return_seller_due_date,
  _airbyte_ab_id as _AIRBYTE_AB_ID,
  _airbyte_emitted_at as _AIRBYTE_EMITTED_AT,
  {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
  from {{ source( 'shopee','return' )}}


   where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}