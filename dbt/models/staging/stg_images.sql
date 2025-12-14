select
  id as image_id,
  breed_id,
  url,
  width,
  height,
  created_at,
  loaded_at
from {{ source('raw', 'images') }}
