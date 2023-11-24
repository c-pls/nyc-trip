INSERT INTO
 {{ params.target_table }}(
    HVFHS_LICENSE_KEY,
    PICKUP_DATE_KEY,
    DROPOFF_DATE_KEY,
    REQUEST_DATE_KEY,
    ON_SCENE_DATE_KEY,
    PICK_UP_LOCATION_KEY,
    DROP_OFF_LOCATION_KEY,
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
) 
  SELECT 
    HVFHS_LICENSE_KEY,
    PICKUP_DATE_KEY,
    DROPOFF_DATE_KEY,
    REQUEST_DATE_KEY,
    ON_SCENE_DATE_KEY,
    PICK_UP_LOCATION_KEY,
    DROP_OFF_LOCATION_KEY,
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
    FROM 
        {{ params.source_table }}