-- MySQL dump 10.13  Distrib 8.0.38, for Win64 (x86_64)
--
-- Host: localhost    Database: lung_caner_air_pollution_warehouse
-- ------------------------------------------------------
-- Server version	8.0.38

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Temporary view structure for view `air_indexes_datamart`
--

DROP TABLE IF EXISTS `air_indexes_datamart`;
/*!50001 DROP VIEW IF EXISTS `air_indexes_datamart`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `air_indexes_datamart` AS SELECT 
 1 AS `country_name`,
 1 AS `date`,
 1 AS `pm25_index`,
 1 AS `pm10_index`,
 1 AS `no2_index`,
 1 AS `so2_index`,
 1 AS `o3_index`,
 1 AS `air_quality_index`*/;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `dim_air_qualities`
--

DROP TABLE IF EXISTS `dim_air_qualities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_air_qualities` (
  `air_quality_id` int NOT NULL AUTO_INCREMENT,
  `air_quality` varchar(225) NOT NULL,
  `air_quality_range` varchar(225) NOT NULL,
  PRIMARY KEY (`air_quality_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_cancer_stages`
--

DROP TABLE IF EXISTS `dim_cancer_stages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_cancer_stages` (
  `cancer_stage_id` varchar(45) NOT NULL,
  `cancer_stage` varchar(225) NOT NULL,
  PRIMARY KEY (`cancer_stage_id`),
  UNIQUE KEY `cancer_stage_id_UNIQUE` (`cancer_stage_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_countries`
--

DROP TABLE IF EXISTS `dim_countries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_countries` (
  `country_code` varchar(45) NOT NULL,
  `country_name` varchar(225) NOT NULL,
  `region_id` varchar(45) NOT NULL,
  PRIMARY KEY (`country_code`),
  UNIQUE KEY `country_code_UNIQUE` (`country_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_dates`
--

DROP TABLE IF EXISTS `dim_dates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_dates` (
  `date_id` int NOT NULL AUTO_INCREMENT,
  `date` varchar(45) NOT NULL,
  `year` int NOT NULL,
  `month` varchar(45) NOT NULL,
  `quarter` int NOT NULL,
  PRIMARY KEY (`date_id`),
  UNIQUE KEY `date_UNIQUE` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=146 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_regions`
--

DROP TABLE IF EXISTS `dim_regions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_regions` (
  `region_id` varchar(45) NOT NULL,
  `region_name` varchar(225) NOT NULL,
  PRIMARY KEY (`region_id`),
  UNIQUE KEY `region_id_UNIQUE` (`region_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_smoking_status`
--

DROP TABLE IF EXISTS `dim_smoking_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_smoking_status` (
  `smoking_status_id` int NOT NULL AUTO_INCREMENT,
  `smoking_status` varchar(225) NOT NULL,
  PRIMARY KEY (`smoking_status_id`),
  UNIQUE KEY `somking_status_id_UNIQUE` (`smoking_status_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_treatment_types`
--

DROP TABLE IF EXISTS `dim_treatment_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_treatment_types` (
  `treatment_type_id` int NOT NULL AUTO_INCREMENT,
  `treatment` varchar(225) NOT NULL,
  PRIMARY KEY (`treatment_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_patient_air_information`
--

DROP TABLE IF EXISTS `fact_patient_air_information`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_patient_air_information` (
  `patient_id` int NOT NULL,
  `patient_age` int NOT NULL,
  `patient_gender` varchar(45) NOT NULL,
  `country_code` varchar(45) NOT NULL,
  `diagnosis_date_id` int NOT NULL,
  `end_of_treatment_date_id` int NOT NULL,
  `cancer_stage_id` varchar(45) NOT NULL,
  `smoking_status_id` int DEFAULT NULL,
  `hypertension` tinyint NOT NULL,
  `asthma` tinyint NOT NULL,
  `cirrhosis` tinyint NOT NULL,
  `treatment_type_id` int NOT NULL,
  `survived` tinyint NOT NULL,
  `pm25_index` float DEFAULT NULL,
  `pm10_index` float DEFAULT NULL,
  `no2_index` float DEFAULT NULL,
  `so2_index` float DEFAULT NULL,
  `o3_index` float DEFAULT NULL,
  `air_quality_index` float DEFAULT NULL,
  `air_quality_id` int DEFAULT NULL,
  PRIMARY KEY (`patient_id`),
  UNIQUE KEY `patient_id_UNIQUE` (`patient_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary view structure for view `lung_cancer_patients_datamart`
--

DROP TABLE IF EXISTS `lung_cancer_patients_datamart`;
/*!50001 DROP VIEW IF EXISTS `lung_cancer_patients_datamart`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `lung_cancer_patients_datamart` AS SELECT 
 1 AS `id`,
 1 AS `age`,
 1 AS `gender`,
 1 AS `country`,
 1 AS `diagnosis_date`,
 1 AS `cancer_stage`,
 1 AS `beginning_of_treatment_date`,
 1 AS `family_history`,
 1 AS `smoking_status`,
 1 AS `bmi`,
 1 AS `cholesterol_level`,
 1 AS `hypertension`,
 1 AS `asthma`,
 1 AS `cirrhosis`,
 1 AS `other_cancer`,
 1 AS `treatment_type`,
 1 AS `end_treatment_date`,
 1 AS `survived`,
 1 AS `year`*/;
SET character_set_client = @saved_cs_client;

--
-- Final view structure for view `air_indexes_datamart`
--

/*!50001 DROP VIEW IF EXISTS `air_indexes_datamart`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `air_indexes_datamart` AS select `c`.`country_name` AS `country_name`,`d`.`date` AS `date`,`f`.`pm25_index` AS `pm25_index`,`f`.`pm10_index` AS `pm10_index`,`f`.`no2_index` AS `no2_index`,`f`.`so2_index` AS `so2_index`,`f`.`o3_index` AS `o3_index`,`f`.`air_quality_index` AS `air_quality_index` from ((`fact_patient_air_information` `f` join `dim_countries` `c` on((`f`.`country_code` = `c`.`country_code`))) join `dim_dates` `d` on((`f`.`diagnosis_date_id` = `d`.`date_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `lung_cancer_patients_datamart`
--

/*!50001 DROP VIEW IF EXISTS `lung_cancer_patients_datamart`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `lung_cancer_patients_datamart` AS select `lung_cancer_air_pollution_staging_area`.`lung_cancer`.`id` AS `id`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`age` AS `age`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`gender` AS `gender`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`country` AS `country`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`diagnosis_date` AS `diagnosis_date`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`cancer_stage` AS `cancer_stage`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`beginning_of_treatment_date` AS `beginning_of_treatment_date`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`family_history` AS `family_history`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`smoking_status` AS `smoking_status`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`bmi` AS `bmi`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`cholesterol_level` AS `cholesterol_level`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`hypertension` AS `hypertension`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`asthma` AS `asthma`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`cirrhosis` AS `cirrhosis`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`other_cancer` AS `other_cancer`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`treatment_type` AS `treatment_type`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`end_treatment_date` AS `end_treatment_date`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`survived` AS `survived`,`lung_cancer_air_pollution_staging_area`.`lung_cancer`.`year` AS `year` from `lung_cancer_air_pollution_staging_area`.`lung_cancer` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-09-21 20:44:30
