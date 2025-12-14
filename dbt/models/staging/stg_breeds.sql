select
  id as breed_id,
  name,
  origin,
  temperament,
  life_span,
  loaded_at
from {{ source('raw', 'breeds') }}
