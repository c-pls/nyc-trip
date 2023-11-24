  SELECT 
    hld.hvfhs_license_key HVFHS_LICENSE_KEY,
    dd_pu.date_key PICKUP_DATE_KEY,
    dd_do.date_key DROPOFF_DATE_KEY,
    dd_r.date_key REQUEST_DATE_KEY,
    dd_oc.date_key ON_SCENE_DATE_KEY,
    tzd_pu.taxi_zone_key PICK_UP_LOCATION_KEY,
    tzd_do.taxi_zone_key DROP_OFF_LOCATION_KEY,
    PICKUP_DATETIME,
	DROPOFF_DATETIME,
    REQUEST_DATETIME,
	ON_SCENE_DATETIME,
    DISPATCHING_BASE_NUM,
	ORIGINATING_BASE_NUM,
	TRIP_MILES,
	TRIP_TIME,
	BASE_PASSENGER_FARE,
	TOLLS,
	BCF,
	SALES_TAX,
	CONGESTION_SURCHARGE,
	AIRPORT_FEE,
	TIPS,
	DRIVER_PAY,
	SHARED_REQUEST_FLAG,
	SHARED_MATCH_FLAG,
	ACCESS_A_RIDE_FLAG,
	WAV_REQUEST_FLAG,
	WAV_MATCH_FLAG
    FROM NYC_TRIP.RAW.FHVHV_TRIP_RAW rt
    INNER JOIN NYC_TRIP.PROD.HVFHS_LICENSE_DIMENSION hld ON rt.hvfhs_license_num = hld.hvfhs_license_id
    INNER JOIN NYC_TRIP.PROD.TAXI_ZONE_DIMENSION tzd_pu ON rt.pulocationid = tzd_pu.locationid
    INNER JOIN NYC_TRIP.PROD.TAXI_ZONE_DIMENSION tzd_do ON rt.dolocationid = tzd_do.locationid
    INNER JOIN NYC_TRIP.PROD.DATE_DIMENSION dd_pu ON TO_DATE(rt.pickup_datetime) = dd_pu.date
    INNER JOIN NYC_TRIP.PROD.DATE_DIMENSION dd_do ON TO_DATE(rt.dropoff_datetime) = dd_do.date
    INNER JOIN NYC_TRIP.PROD.DATE_DIMENSION dd_r ON TO_DATE(rt.request_datetime) = dd_r.date
    LEFT JOIN NYC_TRIP.PROD.DATE_DIMENSION dd_oc ON TO_DATE(rt.on_scene_datetime) = dd_oc.date