SELECT
    fire_district,
    pwd_district,
    city_council_district,
    police_district,
    neighborhood,
    reason,
    type,
    on_time,
    duration_hours
FROM
    city_services_boston.ml.case_duration_ml
ORDER BY
    created_at DESC
LIMIT 10000;