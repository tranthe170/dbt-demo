{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    tags = [ "top-level-intermediate" ]
) }}

select {{
    dbt_utils.surrogate_key([
    'amount', 'reason', 'status', 'order_sn', 'refund_sn', 'buyer_name', 'create_time', 'wallet_type',
    'withdrawal_id', 'transaction_id', 'current_balance', 'transaction_fee', 'withdrawal_type', 'transaction_type',
    'root_withdrawal_id', '_AIRBYTE_AB_ID', '_AIRBYTE_EMITTED_AT', '_AIRBYTE_NORMALIZED_AT'
])
}} as _AIRBYTE_WALLET_TRANSACTION_HASHID,
    tmp.*
from {{ref('wallet_transaction_ab')}} tmp

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}