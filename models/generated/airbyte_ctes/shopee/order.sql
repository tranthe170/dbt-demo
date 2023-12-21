{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    tags = [ "top-level-intermediate" ]
) }}


select
    (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'cod')::boolean as cod,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'note')::text as note,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'region')::text as region,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'currency')::text as currency,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'order_sn')::text as order_sn,
  	to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'pay_time')::numeric) AT TIME ZONE 'UTC' as pay_time,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'split_up')::boolean as split_up,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'cancel_by')::text as cancel_by,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->>'item_list')::json as order_items,
  	to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'create_time')::numeric) AT TIME ZONE 'UTC' as create_time,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'dropshipper')::text as dropshipper,
  	to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'update_time')::numeric) AT TIME ZONE 'UTC' as update_time,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'buyer_cpf_id')::text as buyer_cpf_id,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'days_to_ship')::numeric as days_to_ship,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'invoice_data')::text as invoice_data,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'order_status')::text as order_status,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'package_list')::json as packages_info,
  	to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'ship_by_date')::numeric) AT TIME ZONE 'UTC' as ship_by_date,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'total_amount')::decimal as total_amount,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'buyer_user_id')::numeric as buyer_user_id,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'cancel_reason')::text as cancel_reason,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'buyer_username')::text as buyer_user_name,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'payment_method')::text as payment_method,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'fulfillment_flag')::text as fulfillment_flag,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'goods_to_declare')::boolean as goods_to_declare,
  	to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'note_update_time')::numeric) AT TIME ZONE 'UTC' as note_update_time,
  	to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'pickup_done_time')::numeric) AT TIME ZONE 'UTC' as pickup_done_time,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'dropshipper_phone')::text as dropshipper_phone,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'city')::text as recipient_city,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'name')::text as recipient_name,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'town')::text as recipient_town,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'phone')::text as recipient_phone,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'state')::text as recipient_state,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'region')::text as recipient_region,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'zipcode')::text as recipient_zipcode,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'district')::text as recipient_district,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' -> 'recipient_address' ->> 'full_address')::text as recipient_full_address,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'actual_shipping_fee')::decimal as actual_shipping_fee,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'buyer_cancel_reason')::text as buyer_cancel_reason,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'reverse_shipping_fee')::decimal as reverse_shipping_fee,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'estimated_shipping_fee')::decimal as estimated_shipping_fee,
  	(_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'actual_shipping_fee_confirmed')::boolean as actual_shipping_fee_confirmed,
    _airbyte_ab_id as _AIRBYTE_AB_ID,
    _airbyte_emitted_at as _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
    from {{source( 'shopee','order' )}}

    where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}