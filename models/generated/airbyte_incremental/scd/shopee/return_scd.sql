{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='return'
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
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('return')) }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('return')) }}
                                group by _AIRBYTE_UNIQUE_KEY
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ","drop view shopee.return_stg"],
    tags = [ "top-level" ]
) }}


with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('return_stg')  }}
    where 1 = 1
    {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
),
new_data_ids as (
    -- build a subset of _AIRBYTE_UNIQUE_KEY from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'create_time',
            'return_sn',
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
        {{ star_intersect(ref('return_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    where _AIRBYTE_ACTIVE_ROW = 1
),
input_data as (
    select {{ dbt_utils.star(ref('return_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('return_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('return_stg')  }}
),
{% endif %}

scd_data as (
    select
      {{ dbt_utils.surrogate_key([
            'create_time',
            'return_sn',
        ]) }} as _AIRBYTE_UNIQUE_KEY,
        return_items, user_email, user_portrait, username, image_return, reason, status, activity,
        currency, due_date, order_sn, return_sn, create_time, negotiation_counter_limit, negotiation_offer_due_date,
        negotiation_latest_solution, negotiation_status, negotiation_latest_offer_amount, negotiation_latest_offer_creator,
        text_reason, update_time, seller_proof_status, seller_evidence_deadline, refund_amount, needs_logistics,
        tracking_number, logistics_status, seller_compensation_amount, seller_compensation_status,
        seller_compensation_due_date, return_ship_due_date, return_pickup_city, return_pickup_name, return_pickup_town,
        return_pickup_phone, return_pickup_state, return_pickup_region, return_pickup_address, return_pickup_zipcode,
        return_pickup_district, amount_before_discount, return_seller_due_date,
        create_time as _AIRBYTE_START_AT,
      lag(create_time) over (
        partition by create_time, return_sn
        order by
            create_time is null asc,
            create_time desc,
            _AIRBYTE_EMITTED_AT desc
      ) as _AIRBYTE_END_AT,
      case when row_number() over (
        partition by create_time, return_sn
        order by
            create_time is null asc,
            create_time desc,
            _AIRBYTE_EMITTED_AT desc
      ) = 1 then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
      _AIRBYTE_AB_ID,
      _AIRBYTE_EMITTED_AT,
      _AIRBYTE_RETURN_HASHID
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
    return_items, user_email, user_portrait, username, image_return, reason, status, activity,
    currency, due_date, order_sn, return_sn, create_time, negotiation_counter_limit, negotiation_offer_due_date,
    negotiation_latest_solution, negotiation_status, negotiation_latest_offer_amount, negotiation_latest_offer_creator,
    text_reason, update_time, seller_proof_status, seller_evidence_deadline, refund_amount, needs_logistics,
    tracking_number, logistics_status, seller_compensation_amount, seller_compensation_status,
    seller_compensation_due_date, return_ship_due_date, return_pickup_city, return_pickup_name, return_pickup_town,
    return_pickup_phone, return_pickup_state, return_pickup_region, return_pickup_address, return_pickup_zipcode,
    return_pickup_district, amount_before_discount, return_seller_due_date,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_RETURN_HASHID
from dedup_data where _AIRBYTE_ROW_NUM = 1