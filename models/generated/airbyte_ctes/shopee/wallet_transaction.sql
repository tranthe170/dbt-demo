{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    tags = [ "top-level-intermediate" ]
) }}


SELECT
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'amount')::numeric AS amount,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'reason')::text AS reason,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'status')::text AS status,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'order_sn')::text AS order_sn,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'refund_sn')::text AS refund_sn,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'buyer_name')::text AS buyer_name,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'create_time')::numeric) AT TIME ZONE 'UTC' AS create_time,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'wallet_type')::text AS wallet_type,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'withdrawal_id')::numeric AS withdrawal_id,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'transaction_id')::numeric AS transaction_id,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'current_balance')::numeric AS current_balance,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'transaction_fee')::numeric AS transaction_fee,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'withdrawal_type')::text AS withdrawal_type,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'transaction_type')::text AS transaction_type,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'root_withdrawal_id')::numeric AS root_withdrawal_id,
  _airbyte_ab_id as _AIRBYTE_AB_ID,
  _airbyte_emitted_at as _AIRBYTE_EMITTED_AT,
  {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
  from {{source( 'shopee','wallet_transaction' )}}

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}