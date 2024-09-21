CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `air_indexes_datamart` AS
    SELECT 
        `c`.`country_name` AS `country_name`,
        `d`.`date` AS `date`,
        `f`.`pm25_index` AS `pm25_index`,
        `f`.`pm10_index` AS `pm10_index`,
        `f`.`no2_index` AS `no2_index`,
        `f`.`so2_index` AS `so2_index`,
        `f`.`o3_index` AS `o3_index`,
        `f`.`air_quality_index` AS `air_quality_index`
    FROM
        ((`fact_patient_air_information` `f`
        JOIN `dim_countries` `c` ON ((`f`.`country_code` = `c`.`country_code`)))
        JOIN `dim_dates` `d` ON ((`f`.`diagnosis_date_id` = `d`.`date_id`)))