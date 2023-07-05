-- This model flattens the nested properties of the users collection

WITH nested_users AS (
  SELECT
    _id,
    name,
    globalCustomerId,
    phoneNumbers,
    isPrivateAccount,
    createdAt,
    updatedAt,
    ROW_NUMBER() OVER (ORDER BY _id) AS user_row_number -- Add a row number for joining with nested kid records
  FROM mongodb_users
)

SELECT
  nested_users._id AS user_id,
  nested_users.name,
  nested_users.globalCustomerId,
  unnested_phonenumbers.value AS phone_number,
  nested_users.isPrivateAccount,
  nested_users.createdAt,
  nested_users.updatedAt
FROM nested_users
LEFT JOIN UNNEST(nested_users.phoneNumbers) AS unnested_phonenumbers
ORDER BY nested_users.user_row_number;
