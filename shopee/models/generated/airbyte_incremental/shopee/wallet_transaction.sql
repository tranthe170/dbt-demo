{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    schema = "kiotviet",
    tags = [ "top-level" ]
) }}

select
    _AIRBYTE_UNIQUE_KEY,
    amount, reason, status, order_sn, refund_sn, buyer_name, create_time, wallet_type,
    withdrawal_id, transaction_id, current_balance, transaction_fee, withdrawal_type, transaction_type,
    root_withdrawal_id,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_WALLET_TRANSACTION_HASHID
from {{ ref('wallet_transaction_scd') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}