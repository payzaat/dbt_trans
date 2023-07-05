-- This model flattens the nested properties of the kids collection

WITH nested_kids AS (
  SELECT
    nested_users._id AS user_id, -- Join with the flattened users collection
    kids._id AS kid_id,
    kids.name AS kid_name,
    kids.gender,
    kids.birthDate AS birth_date,
    kids.avatarId AS avatar_id,
    kids.createdAt AS created_at,
    kids.updatedAt AS updated_at,
    ROW_NUMBER() OVER (PARTITION BY nested_users._id ORDER BY kids._id) AS kid_row_number -- Add a row number for joining with nested wishlist records
  FROM mongodb_users
  LEFT JOIN UNNEST(kids) AS kids
  LEFT JOIN unnest_users ON nested_users._id = kids.parent_id
)

SELECT
  nested_kids.user_id,
  nested_kids.kid_id,
  nested_kids.kid_name,
  nested_kids.gender,
  nested_kids.birth_date,
  nested_kids.avatar_id,
  nested_kids.created_at,
  nested_kids.updated_at,
  nested_wishlist.sku,
  nested_wishlist.sortOrder,
  nested_wishlist.isPurchased,
  nested_wishlist.addedAt
FROM nested_kids
LEFT JOIN UNNEST(nested_kids.wishList.products) AS nested_wishlist
ORDER BY nested_kids.kid_row_number;
