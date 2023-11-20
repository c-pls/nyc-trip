COPY INTO {table}
FROM(
    SELECT
  $1:VendorID ::INTEGER AS VendorID,
  TO_TIMESTAMP_NTZ($1:lpep_pickup_datetime::VARCHAR) AS lpep_pickup_datetime,
  TO_TIMESTAMP_NTZ($1:lpep_dropoff_datetime::VARCHAR) AS lpep_dropoff_datetime,
  $1:store_and_fwd_flag::STRING AS store_and_fwd_flag,
  $1:RatecodeID::FLOAT AS RatecodeID,
  $1:PULocationID::INT AS PULocationID,
  $1:DOLocationID::INT AS DOLocationID,
  $1:passenger_count::FLOAT AS passenger_count,
  $1:trip_distance::FLOAT AS trip_distance,
  $1:fare_amount::FLOAT AS fare_amount,
  $1:extra::FLOAT AS extra,
  $1:mta_tax::FLOAT AS mta_tax,
  $1:tip_amount::FLOAT AS tip_amount,
  $1:tolls_amount::FLOAT AS tolls_amount,
  $1:ehail_fee::INT AS ehail_fee,
  $1:improvement_surcharge::FLOAT AS improvement_surcharge,
  $1:total_amount::FLOAT AS total_amount,
  $1:payment_type::FLOAT AS payment_type,
  $1:trip_type::FLOAT AS trip_type,
  $1:congestion_surcharge::FLOAT AS congestion_surcharge

  FROM @{stage_name}
);