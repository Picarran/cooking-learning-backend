-- MySQL schema for cooking project
-- Run in your MySQL server (adjust database name and charset as needed)
CREATE DATABASE IF NOT EXISTS `cooking` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `cooking`;

-- recipes table
DROP TABLE IF EXISTS `recipes`;
CREATE TABLE `recipes` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `dish_name` VARCHAR(255) NOT NULL,
  `description` TEXT,
  `difficulty` INT,
  `servings` VARCHAR(255),
  `category` VARCHAR(255),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dish_name` (`dish_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- recipe images
DROP TABLE IF EXISTS `recipe_images`;
CREATE TABLE `recipe_images` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `image_url` VARCHAR(1000),
  PRIMARY KEY (`id`),
  KEY `idx_recipe_images_recipe_id` (`recipe_id`),
  CONSTRAINT `fk_recipe_images_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- required ingredients
DROP TABLE IF EXISTS `required_ingredients`;
CREATE TABLE `required_ingredients` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `amount` VARCHAR(255),
  `note` VARCHAR(500),
  PRIMARY KEY (`id`),
  KEY `idx_required_recipe_id` (`recipe_id`),
  CONSTRAINT `fk_required_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- optional ingredients
DROP TABLE IF EXISTS `optional_ingredients`;
CREATE TABLE `optional_ingredients` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `amount` VARCHAR(255),
  `note` VARCHAR(500),
  PRIMARY KEY (`id`),
  KEY `idx_optional_recipe_id` (`recipe_id`),
  CONSTRAINT `fk_optional_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- steps table
DROP TABLE IF EXISTS `steps`;
CREATE TABLE `steps` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `step_number` INT,
  `description` TEXT,
  `time_duration` VARCHAR(255),
  `time_type` VARCHAR(255),
  `target_condition` VARCHAR(1000),
  `is_blockable` TINYINT(1),
  `heat_level` VARCHAR(255),
  `note` VARCHAR(500),
  PRIMARY KEY (`id`),
  KEY `idx_steps_recipe_id` (`recipe_id`),
  CONSTRAINT `fk_steps_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- feedback table
DROP TABLE IF EXISTS `feedback`;
CREATE TABLE `feedback` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT,
  `rating` INT,
  `comment` VARCHAR(2000),
  `image_url` VARCHAR(1000),
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
