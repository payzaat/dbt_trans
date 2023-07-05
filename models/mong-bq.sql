-- models/flatten_customers.sql

-- Transformation to flatten the customers collection

-- Clearing existing data in the destination table
{{ config(materialized='drop') }}

-- Creating the flattened customers table
WITH flattened_customers AS (
  SELECT
    "_id" AS customer_id,
    "name" AS customer_name,
    "globalCustomerId" AS global_customer_id,
    "firebaseTokens" AS firebase_tokens,
    "phoneNumbers" AS phone_numbers,
    "kids" AS kids,
    "createdAt" AS created_at,
    "updatedAt" AS updated_at,
    "isPrivateAccount" AS is_private_account
  FROM "{{ source('mongodb_source', 'customers') }}"
),

flattened_kids AS (
  SELECT
    customer_id AS parent_customer_id,
    kid._id AS kid_id,
    kid.name AS kid_name,
    kid.gender AS kid_gender,
    kid.birthDate AS kid_birth_date,
    kid.avatarId AS kid_avatar_id,
    kid.createdAt AS kid_created_at,
    kid.updatedAt AS kid_updated_at
  FROM flattened_customers,
    UNNEST(kids) AS kid
),

flattened_wishlist AS (
  SELECT
    kid_id AS parent_kid_id,
    wish._id AS wishlist_id,
    wish.sku AS product_sku,
    wish.sortOrder AS product_sort_order,
    wish.isPurchased AS product_is_purchased,
    wish.addedAt AS product_added_at
  FROM flattened_kids,
    UNNEST(kids) AS kid,
    UNNEST(kid.wishList.products) AS wish
)

SELECT
  customer_id,
  customer_name,
  global_customer_id,
  firebase_tokens,
  phone_numbers,
  kid_id,
  kid_name,
  kid_gender,
  kid_birth_date,
  kid_avatar_id,
  kid_created_at,
  kid_updated_at,
  wishlist_id,
  product_sku,
  product_sort_order,
  product_is_purchased,
  product_added_at,
  created_at,
  updated_at,
  is_private_account
FROM flattened_wishlist;
