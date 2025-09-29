CREATE TABLE IF NOT EXISTS Weather_Data (
    EventId          VARCHAR(50) PRIMARY KEY,
    Type             VARCHAR(50),
    Severity         VARCHAR(50),
    StartTimeUTC     TIMESTAMP,
    EndTimeUTC       TIMESTAMP,
    PrecipitationIn  NUMERIC(7,2),
    TimeZone         VARCHAR(50),
    AirportCode      VARCHAR(10),
    LocationLat      NUMERIC(9,6),
    LocationLng      NUMERIC(9,6),
    City             VARCHAR(100),
    County           VARCHAR(100),
    State            CHAR(2),
    ZipCode          VARCHAR(10)
);