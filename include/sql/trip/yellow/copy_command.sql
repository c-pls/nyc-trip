COPY INTO {table}
FROM
    (
        SELECT
            CAST($1:VendorID AS INTEGER) AS VendorID,
            TO_TIMESTAMP_NTZ($1:tpep_pickup_datetime::VARCHAR) AS tpep_pickup_datetime,
            TO_TIMESTAMP_NTZ($1:tpep_dropoff_datetime::VARCHAR) AS tpep_dropoff_datetime,
            CAST($1:passenger_count AS DOUBLE) AS passenger_count,
            CAST($1:trip_distance AS DOUBLE) AS trip_distance,
            CAST($1:RatecodeID AS DOUBLE) AS RatecodeID,
            CAST($1:store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
            CAST($1:PULocationID AS INTEGER) AS PULocationID,
            CAST($1:DOLocationID AS INTEGER) AS DOLocationID,
            CAST($1:payment_type AS INTEGER) AS payment_type,
            CAST($1:fare_amount AS DOUBLE) AS fare_amount,
            CAST($1:extra AS DOUBLE) AS extra,
            CAST($1:mta_tax AS DOUBLE) AS mta_tax,
            CAST($1:tip_amount AS DOUBLE) AS tip_amount,
            CAST($1:tolls_amount AS DOUBLE) AS tolls_amount,
            CAST($1:improvement_surcharge AS DOUBLE) AS improvement_surcharge,
            CAST($1:total_amount AS DOUBLE) AS total_amount,
            CAST($1:congestion_surcharge AS DOUBLE) AS congestion_surcharge,
            CAST($1:airport_fee AS DOUBLE) AS airport_fee
        FROM
            @{stage_name}
    );