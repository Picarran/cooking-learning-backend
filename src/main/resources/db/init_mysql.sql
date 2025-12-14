-- MySQL schema for cooking project (synchronized with docs/db_tables.md)
-- Run in your MySQL server (adjust database name and charset as needed)

-- Drop child tables first to avoid FK errors
DROP TABLE IF EXISTS `recipe_views`;
DROP TABLE IF EXISTS `cooking_records`;
DROP TABLE IF EXISTS `steps`;
DROP TABLE IF EXISTS `required_ingredients`;
DROP TABLE IF EXISTS `optional_ingredients`;
DROP TABLE IF EXISTS `recipe_images`;
DROP TABLE IF EXISTS `recipes`;
DROP TABLE IF EXISTS `users`;
DROP TABLE IF EXISTS `courses`;


-- users table
CREATE TABLE `users` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `openid` VARCHAR(128) NOT NULL COMMENT '微信 openid，唯一',
  `nickname` VARCHAR(64) DEFAULT NULL,
  `avatar_url` VARCHAR(512) DEFAULT NULL,
  `points` BIGINT NOT NULL DEFAULT 0,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_users_openid` (`openid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- recipes table
CREATE TABLE `recipes` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `dish_name` VARCHAR(255) NOT NULL,
  `images` VARCHAR(1000) NULL,
  `description` TEXT,
  `owner_id` BIGINT NULL COMMENT '上传者 user_id，系统导入的数据为 NULL',
  `difficulty` TINYINT NOT NULL DEFAULT 1,
  `servings` VARCHAR(64) DEFAULT NULL,
  `category` VARCHAR(64) DEFAULT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_recipes_owner` (`owner_id`),
  CONSTRAINT `fk_recipes_owner` FOREIGN KEY (`owner_id`) REFERENCES `users` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- required ingredients
CREATE TABLE `required_ingredients` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `amount` VARCHAR(255) DEFAULT NULL,
  `note` VARCHAR(500) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_required_ing_recipe` (`recipe_id`),
  CONSTRAINT `fk_required_ing_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- optional ingredients
CREATE TABLE `optional_ingredients` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `amount` VARCHAR(255) DEFAULT NULL,
  `note` VARCHAR(500) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_optional_ing_recipe` (`recipe_id`),
  CONSTRAINT `fk_optional_ing_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- steps table
CREATE TABLE `steps` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `step_number` INT,
  `image_url` VARCHAR(1000) NULL,
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

-- recipe views
CREATE TABLE `recipe_views` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `user_id` BIGINT DEFAULT NULL,
  `recipe_id` BIGINT NOT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_recipe_views_recipe` (`recipe_id`),
  CONSTRAINT `fk_recipe_views_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_recipe_views_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- cooking records
CREATE TABLE `cooking_records` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `user_id` BIGINT NOT NULL,
  `recipe_id` BIGINT NOT NULL,
  `started_at` DATETIME DEFAULT NULL,
  `finished_at` DATETIME DEFAULT NULL,
  `rating` TINYINT DEFAULT NULL,
  `notes` TEXT DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_cooking_records_user` (`user_id`),
  KEY `idx_cooking_records_recipe` (`recipe_id`),
  CONSTRAINT `fk_cooking_records_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_cooking_records_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes` (`id`) ON DELETE CASCADE
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

-- courses table
CREATE TABLE `courses` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '课程ID',
  `title` VARCHAR(255) NOT NULL COMMENT '课程标题',
  `description` TEXT COMMENT '课程描述',
  `cover_url` VARCHAR(512) DEFAULT NULL COMMENT '封面图URL',
  `video_url` VARCHAR(1024) NOT NULL COMMENT '课程视频URL',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='课程表';

INSERT INTO `courses`
(`title`, `description`, `cover_url`, `video_url`)
VALUES
(
  '家常红烧肉教学',
  '适合零基础的新手红烧肉教学课程，讲解选材、火候与调味技巧。',
  'https://example.com/images/courses/hongshaorou.jpg',
  'https://example.com/videos/hongshaorou.mp4'
),
(
  '5分钟快手早餐',
  '上班族必备的快手早餐课程，健康、省时、不踩雷。',
  'https://example.com/images/courses/breakfast.jpg',
  'https://example.com/videos/breakfast.mp4'
),
(
  '新手厨房刀工入门',
  '从零开始讲解切丝、切片、切丁等基础刀工技巧。',
  'https://example.com/images/courses/knife-skills.jpg',
  'https://example.com/videos/knife-skills.mp4'
);
