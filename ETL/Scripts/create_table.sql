IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ride_records' and xtype='U')
BEGIN

CREATE TABLE ride_records (
    tpep_pickup_datetime  DATETIME2(0) NOT NULL,
    tpep_dropoff_datetime DATETIME2(0) NOT NULL,

    passenger_count       TINYINT NULL,
    trip_distance         FLOAT NULL,

    store_and_fwd_flag    VARCHAR(3) NOT NULL,

    pu_location_id        INT NULL,
    do_location_id        INT NULL,

    fare_amount           DECIMAL(10,2) NULL,
    tip_amount            DECIMAL(10,2) NULL
);

CREATE INDEX IX_ride_records_pu_location_id ON ride_records(pu_location_id);

CREATE INDEX IX_ride_records_trip_distance ON ride_records(trip_distance);

CREATE INDEX IX_ride_records_pickup_dropoff ON ride_records(tpep_pickup_datetime, tpep_dropoff_datetime);

END;