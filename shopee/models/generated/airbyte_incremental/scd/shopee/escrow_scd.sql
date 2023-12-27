{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='escrow'
                        )
                    %}
                    {%
                    if final_table_relation is not none and '_AIRBYTE_UNIQUE_KEY' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    delete from {{ final_table_relation }} where {{ final_table_relation }}._AIRBYTE_UNIQUE_KEY in (
                        select recent_records.unique_key
                        from (
                                select distinct _AIRBYTE_UNIQUE_KEY as unique_key
                                from {{ this }}
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('escrow')) }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('escrow')) }}
                                group by _AIRBYTE_UNIQUE_KEY
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ","drop view shopee.escrow_stg"],
    tags = [ "top-level" ]
) }}


with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('escrow_stg')  }}
    where 1 = 1
    {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
),
new_data_ids as (
    -- build a subset of _AIRBYTE_UNIQUE_KEY from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'order_sn',
        ]) }} as _AIRBYTE_UNIQUE_KEY
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('escrow_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    where _AIRBYTE_ACTIVE_ROW = 1
),
input_data as (
    select {{ dbt_utils.star(ref('escrow_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('escrow_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('escrow_stg')  }}
),
{% endif %}

scd_data as (
    select
      {{ dbt_utils.surrogate_key([
            'order_sn',
        ]) }} as _AIRBYTE_UNIQUE_KEY,
        order_sn, coins, order_items, escrow_tax, service_fee,
        campaign_fee, escrow_amount, commission_fee, instalment_plan,
        cross_border_tax, payment_promotion, buyer_total_amount,
        cost_of_goods_sold, final_shipping_fee, actual_shipping_fee,
        order_selling_price, seller_voucher_code, voucher_from_seller,
        voucher_from_shopee, buyer_payment_method, order_original_price,
        reverse_shipping_fee, seller_return_refund, buyer_transaction_fee,
        credit_card_promotion, drc_adjustable_refund, final_product_vat_tax,
        order_seller_discount, seller_coin_cash_back, estimated_shipping_fee,
        final_shipping_vat_tax, order_discounted_price, seller_transaction_fee,
        shopee_shipping_rebate, buyer_paid_shipping_fee, order_chargeable_weight,
        total_adjustment_amount, final_escrow_product_gst, final_product_protection,
        order_ams_commission_fee, original_shopee_discount, seller_lost_compensation,
        seller_shipping_discount, final_escrow_shipping_gst, credit_card_transaction_fee,
        original_cost_of_goods_sold, escrow_amount_after_adjustment,
        shipping_fee_discount_from_3pl, final_return_to_seller_shipping_fee,
        shipping_seller_protection_fee_amount, fsf_seller_protection_fee_claim_amount,
        rsf_seller_protection_fee_claim_amount, prorated_coins_value_offset_return_items,
        prorated_shopee_voucher_offset_return_items, delivery_seller_protection_fee_premium_amount,
        buyer_user_name, return_order_sn_list, payout_amount, escrow_release_time,
        escrow_release_time as _AIRBYTE_START_AT,
      lag(escrow_release_time) over (
        partition by escrow_release_time, order_sn
        order by
            escrow_release_time is null asc,
            escrow_release_time desc,
            _AIRBYTE_EMITTED_AT desc
      ) as _AIRBYTE_END_AT,
      case when row_number() over (
        partition by escrow_release_time, order_sn
        order by
            escrow_release_time is null asc,
            escrow_release_time desc,
            _AIRBYTE_EMITTED_AT desc
      ) = 1 then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
      _AIRBYTE_AB_ID,
      _AIRBYTE_EMITTED_AT,
      _AIRBYTE_ESCROW_HASHID
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by
                _AIRBYTE_UNIQUE_KEY,
                _AIRBYTE_START_AT,
                _AIRBYTE_EMITTED_AT
            order by _AIRBYTE_ACTIVE_ROW desc, _AIRBYTE_AB_ID
        ) as _AIRBYTE_ROW_NUM,
        {{ dbt_utils.surrogate_key([
          '_AIRBYTE_UNIQUE_KEY',
          '_AIRBYTE_START_AT',
          '_AIRBYTE_EMITTED_AT'
        ]) }} as _AIRBYTE_UNIQUE_KEY_SCD,
        scd_data.*
    from scd_data
)
select
    _AIRBYTE_UNIQUE_KEY,
    _AIRBYTE_UNIQUE_KEY_SCD,
    order_sn, coins, order_items, escrow_tax, service_fee,
    campaign_fee, escrow_amount, commission_fee, instalment_plan,
    cross_border_tax, payment_promotion, buyer_total_amount,
    cost_of_goods_sold, final_shipping_fee, actual_shipping_fee,
    order_selling_price, seller_voucher_code, voucher_from_seller,
    voucher_from_shopee, buyer_payment_method, order_original_price,
    reverse_shipping_fee, seller_return_refund, buyer_transaction_fee,
    credit_card_promotion, drc_adjustable_refund, final_product_vat_tax,
    order_seller_discount, seller_coin_cash_back, estimated_shipping_fee,
    final_shipping_vat_tax, order_discounted_price, seller_transaction_fee,
    shopee_shipping_rebate, buyer_paid_shipping_fee, order_chargeable_weight,
    total_adjustment_amount, final_escrow_product_gst, final_product_protection,
    order_ams_commission_fee, original_shopee_discount, seller_lost_compensation,
    seller_shipping_discount, final_escrow_shipping_gst, credit_card_transaction_fee,
    original_cost_of_goods_sold, escrow_amount_after_adjustment,
    shipping_fee_discount_from_3pl, final_return_to_seller_shipping_fee,
    shipping_seller_protection_fee_amount, fsf_seller_protection_fee_claim_amount,
    rsf_seller_protection_fee_claim_amount, prorated_coins_value_offset_return_items,
    prorated_shopee_voucher_offset_return_items, delivery_seller_protection_fee_premium_amount,
    buyer_user_name, return_order_sn_list, payout_amount, escrow_release_time,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_ESCROW_HASHID
from dedup_data where _AIRBYTE_ROW_NUM = 1