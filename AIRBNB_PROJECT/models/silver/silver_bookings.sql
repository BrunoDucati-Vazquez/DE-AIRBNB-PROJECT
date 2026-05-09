{# Quando usamos o incremental, estamos falando que queremos usar o incremental loading
mas quando passamos o unique_key estamos usando o upsert #}

{{config(materialized='incremental', unique_key='BOOKING_ID')}} 

SELECT
    BOOKING_ID,
    LISTING_ID,
    BOOKING_DATE,
    {{ multiply('NIGHTS_BOOKED', 'BOOKING_AMOUNT', 2) }} + CLEANING_FEE + SERVICE_FEE AS TOTAL_BOOKING_AMOUNT,
    SERVICE_FEE,
    CLEANING_FEE,
    BOOKING_STATUS,
    CREATED_AT
FROM
    {{ ref('bronze_bookings') }}
