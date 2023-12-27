{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    schema = "kiotviet",
    tags = [ "top-level" ]
) }}

select
    _AIRBYTE_UNIQUE_KEY,
    item_id, brand_id, original_brand_name, image_id_list, image_url_list, weight,
    base_info_item_id, item_sku, condition, package_width, package_height, package_length,
    has_model, item_name, days_to_ship, is_pre_order, size_chart, video_info, category_id,
    create_time, update_time, logistic_info, size_chart_id, attributes_info, item_dangerous,
    description_info, sale, likes, views, rating_star, comment_count, item_status,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_PRODUCT_HASHID
from {{ ref('product_scd') }}
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}