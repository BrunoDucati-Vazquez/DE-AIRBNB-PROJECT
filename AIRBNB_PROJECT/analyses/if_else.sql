{% set flag = 2%}

SELECT * FROM {{ ref('bronze_bookings') }}
{% if flag == 1%}
    WHERE nights_booked > 1
{% else %}
    WHERE nights_booked = 1
{% endif %}