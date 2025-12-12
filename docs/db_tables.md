1. 用户表`users`

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


---

4. 菜谱表`recipes`（扩展已有表）

注意：项目已有 recipes 表（或 recipes 实体）。下面给出推荐字段以支持用户上传与审核流程。

CREATE TABLE `recipes` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `dish_name` VARCHAR(255) NOT NULL,
  `description` TEXT,
  `owner_id` BIGINT NULL COMMENT '上传者 user_id，系统生成的为 NULL',
  `difficulty` TINYINT DEFAULT NULL,
  `servings` VARCHAR(64) DEFAULT NULL,
  `category` VARCHAR(64) DEFAULT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_recipes_owner` (`owner_id`),
  CONSTRAINT `fk_recipes_owner` FOREIGN KEY (`owner_id`) REFERENCES `users` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


---

5. 菜谱图片`recipe_images`

CREATE TABLE `recipe_images` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `recipe_id` BIGINT NOT NULL,
  `image_url` VARCHAR(1000),
  PRIMARY KEY (`id`),
  KEY `idx_recipe_images_recipe` (`recipe_id`),
  CONSTRAINT `fk_recipe_images_recipe` FOREIGN KEY (`recipe_id`) REFERENCES `recipes` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


---

6. 配料表（必需/可选）

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


---

7. 步骤表`steps`

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


---

8. 浏览记录`recipe_views`

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


---

9. 做菜记录`cooking_records`

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