CREATE TABLE IF NOT EXISTS `bookmarks`(
`id` bigint(20) NOT NULL AUTO_INCREMENT,
`user_id` bigint(20) NOT NULL,
`thread_id` bigint(20) NOT NULL,
PRIMARY KEY (id),
INDEX (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;