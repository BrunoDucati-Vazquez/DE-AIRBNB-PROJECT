{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH gold_bookings AS 
(
    SELECT 
        BOOKING_ID,
        BOOKING_DATE,
        BOOKING_STATUS,
        CREATED_AT
    FROM 
        {{ ref('obt') }}
)
SELECT * FROM gold_bookings