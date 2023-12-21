{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    tags = [ "top-level-intermediate" ]
) }}

select {{
    dbt_utils.surrogate_key([
    'order_sn', 'coins', 'order_items', 'escrow_tax', 'service_fee',
    'campaign_fee', 'escrow_amount', 'commission_fee', 'instalment_plan',
    'cross_border_tax', 'payment_promotion', 'buyer_total_amount',
    'cost_of_goods_sold', 'final_shipping_fee', 'actual_shipping_fee',
    'order_selling_price', 'seller_voucher_code', 'voucher_from_seller',
    'voucher_from_shopee', 'buyer_payment_method', 'order_original_price',
    'reverse_shipping_fee', 'seller_return_refund', 'buyer_transaction_fee',
    'credit_card_promotion', 'drc_adjustable_refund', 'final_product_vat_tax',
    'order_seller_discount', 'seller_coin_cash_back', 'estimated_shipping_fee',
    'final_shipping_vat_tax', 'order_discounted_price', 'seller_transaction_fee',
    'shopee_shipping_rebate', 'buyer_paid_shipping_fee', 'order_chargeable_weight',
    'total_adjustment_amount', 'final_escrow_product_gst', 'final_product_protection',
    'order_ams_commission_fee', 'original_shopee_discount', 'seller_lost_compensation',
    'seller_shipping_discount', 'final_escrow_shipping_gst', 'credit_card_transaction_fee',
    'original_cost_of_goods_sold', 'escrow_amount_after_adjustment',
    'shipping_fee_discount_from_3pl', 'final_return_to_seller_shipping_fee',
    'shipping_seller_protection_fee_amount', 'fsf_seller_protection_fee_claim_amount',
    'rsf_seller_protection_fee_claim_amount', 'prorated_coins_value_offset_return_items',
    'prorated_shopee_voucher_offset_return_items', 'delivery_seller_protection_fee_premium_amount',
    'buyer_user_name', 'return_order_sn_list', 'payout_amount', 'escrow_release_time',
    '_AIRBYTE_AB_ID', '_AIRBYTE_EMITTED_AT', '_AIRBYTE_NORMALIZED_AT'
])
}} as _AIRBYTE_ESCROW_HASHID,
    tmp.*

from {{ref('escrow')}} tmp

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}