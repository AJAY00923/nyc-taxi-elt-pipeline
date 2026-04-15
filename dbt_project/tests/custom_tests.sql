{% test no_future_dates(model, column_name) %}
/*
  Custom test: ensures no timestamp is in the future.
  Usage in schema.yml:
    tests:
      - no_future_dates
*/

select {{ column_name }}
from {{ model }}
where {{ column_name }} > current_timestamp()

{% endtest %}


{% test positive_values(model, column_name) %}
/*
  Custom test: ensures a numeric column has no negative values.
  Usage in schema.yml:
    tests:
      - positive_values
*/

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}


{% test reasonable_trip_duration(model, min_minutes=1, max_minutes=300) %}
/*
  Custom test: trip duration must be between min and max minutes.
  Catches data where pickup and dropoff times are swapped or corrupted.
*/

select trip_duration_minutes
from {{ model }}
where trip_duration_minutes < {{ min_minutes }}
   or trip_duration_minutes > {{ max_minutes }}

{% endtest %}
