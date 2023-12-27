{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    tags = [ "top-level-intermediate" ]
) }}


SELECT
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'item_id')::numeric AS item_id,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'brand' ->> 'brand_id')::numeric AS brand_id,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'brand' ->> 'original_brand_name')::text AS original_brand_name,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'image' ->> 'image_id_list')::json AS image_id_list,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'image' ->> 'image_url_list')::json AS image_url_list,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'weight')::text AS weight,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'item_id')::numeric AS base_info_item_id,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'item_sku')::text AS item_sku,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'condition')::text AS condition,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'dimension' ->> 'package_width')::numeric AS package_width,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'dimension' ->> 'package_height')::numeric AS package_height,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'dimension' ->> 'package_length')::numeric AS package_length,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'has_model')::boolean AS has_model,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'item_name')::text AS item_name,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'pre_order' ->> 'days_to_ship')::numeric AS days_to_ship,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' -> 'pre_order' ->> 'is_pre_order')::boolean AS is_pre_order,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'size_chart')::text AS size_chart,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->>'video_info')::json as video_info,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'category_id')::numeric AS category_id,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data'->'base_info' ->> 'create_time')::numeric) AT TIME ZONE 'UTC' as create_time,
  to_timestamp((_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'update_time')::numeric) AT TIME ZONE 'UTC' AS update_time,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'logistic_info')::json as logistic_info,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'size_chart_id')::text AS size_chart_id,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'attribute_list')::json as attributes_info,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'item_dangerous')::int AS item_dangerous,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'base_info' ->> 'description_info')::json AS description_info,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'extra_info' ->> 'sale')::numeric AS sale,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'extra_info' ->> 'likes')::numeric AS likes,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'extra_info' ->> 'views')::numeric AS views,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'extra_info' ->> 'rating_star')::decimal AS rating_star,
  (_airbyte_data::json ->'data'-> '_airbyte_data' -> 'extra_info' ->> 'comment_count')::numeric AS comment_count,
  (_airbyte_data::json ->'data'-> '_airbyte_data' ->> 'item_status' )::text AS item_status,
  _airbyte_ab_id as _AIRBYTE_AB_ID,
  _airbyte_emitted_at as _AIRBYTE_EMITTED_AT,
  {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
  from {{source( 'shopee','product' )}}

  where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}