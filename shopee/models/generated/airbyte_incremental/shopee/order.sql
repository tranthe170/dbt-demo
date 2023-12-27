{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    schema = "kiotviet",
    tags = [ "top-level" ]
) }}

select
    _AIRBYTE_UNIQUE_KEY,
    cod, note, region, currency, order_sn, pay_time, split_up, cancel_by,
    order_items, create_time, dropshipper, update_time, buyer_cpf_id, days_to_ship,
    invoice_data, order_status, packages_info, ship_by_date, total_amount,
    buyer_user_id, cancel_reason, buyer_user_name, payment_method, fulfillment_flag,
    goods_to_declare, note_update_time, pickup_done_time, dropshipper_phone,
    recipient_city, recipient_name, recipient_town, recipient_phone, recipient_state,
    recipient_region, recipient_zipcode, recipient_district, recipient_full_address,
    actual_shipping_fee, buyer_cancel_reason, reverse_shipping_fee, estimated_shipping_fee,
    actual_shipping_fee_confirmed,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_ORDER_HASHID
from {{ ref('order_scd') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}