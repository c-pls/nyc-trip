    SELECT
    tzd_pu.taxi_zone_key AS pick_up_location_key,
    tzd_do.taxi_zone_key AS drop_off_location_key,
    rcd.rate_code_key AS rate_code_key,
    vd.vendor_key AS vendor_key,
    pyd.payment_type_key payment_type_key,
    dd_pu.date_key lpep_pickup_date_key,
    dd_do.date_key lpep_dropoff_date_key,
    ttd.trip_type_key trip_type_key,
    LPEP_PICKUP_DATETIME,
    LPEP_DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    STORE_AND_FWD_FLAG,
    FARE_AMOUNT,
    EXTRA,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    IMPROVEMENT_SURCHARGE,
    TOTAL_AMOUNT
FROM
     GREEN_TRIP_RECORD_RAW rt
    INNER JOIN NYC_TRIP.STAGING.TAXI_ZONE_DIMENSION tzd_pu ON rt.pulocationid = tzd_pu.locationid
    INNER JOIN NYC_TRIP.STAGING.TAXI_ZONE_DIMENSION tzd_do ON rt.dolocationid = tzd_do.locationid
    INNER JOIN NYC_TRIP.STAGING.DATE_DIMENSION dd_pu ON TO_DATE(rt.lpep_pickup_datetime) = dd_pu.date
    INNER JOIN NYC_TRIP.STAGING.DATE_DIMENSION dd_do ON TO_DATE(rt.lpep_dropoff_datetime) = dd_do.date
    INNER JOIN NYC_TRIP.STAGING.RATE_CODE_DIMENSION rcd ON rt.RATECODEID = rcd.rate_code_id
    INNER JOIN NYC_TRIP.STAGING.VENDOR_DIMENSION vd ON rt.vendorid = vd.vendor_id
    INNER JOIN NYC_TRIP.STAGING.PAYMENT_TYPE_DIMENSION pyd ON rt.payment_type = pyd.payment_type_id
    INNER JOIN NYC_TRIP.STAGING.TRIP_TYPE_DIMENSION ttd ON rt.trip_type = ttd.trip_type_id