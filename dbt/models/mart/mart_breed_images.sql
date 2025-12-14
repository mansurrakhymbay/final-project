select
  b.breed_id,
  b.name as breed_name,
  b.origin,
  count(i.image_id) as images_count,
  max(i.created_at) as last_image_created_at
from {{ ref('stg_breeds') }} b
left join {{ ref('stg_images') }} i
  on b.breed_id = i.breed_id
group by 1,2,3
