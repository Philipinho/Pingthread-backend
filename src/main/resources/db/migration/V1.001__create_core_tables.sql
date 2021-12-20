CREATE TABLE IF NOT EXISTS `authors` (
`id` bigint(20) AUTO_INCREMENT NOT NULL,
`user_id` bigint(200) NOT NULL,
`username` varchar(20) NOT NULL,
`name` varchar(150) NOT NULL,
`bio` varchar(1000) DEFAULT NULL,
`profile_picture` varchar(200) DEFAULT NULL,
`verified` int(1) DEFAULT NULL,
`sponsor_platform` varchar(250) DEFAULT NULL,
`sponsor_url` varchar(250) DEFAULT NULL,
`status` int(1) DEFAULT 1,
`time_created` datetime DEFAULT CURRENT_TIMESTAMP,
`author_updated` datetime NULL DEFAULT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY (`user_id`),
INDEX (`username`)
)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `threads` (
`id` bigint(20) AUTO_INCREMENT NOT NULL,
`author_user_id` bigint(20) NOT NULL,
`thread_id` bigint(20) NOT NULL,
`thread_snippet` varchar(3000) NOT NULL,
`thread_text` MEDIUMTEXT NOT NULL,
`thread_count` int(11) NOT NULL,
`hashtags` varchar(3000) DEFAULT NULL,
`thread_lang` varchar(100) DEFAULT NULL,
`thread_views` bigint(20) DEFAULT  NULL,
`tweet_created` datetime DEFAULT CURRENT_TIMESTAMP,
`time_saved` datetime DEFAULT CURRENT_TIMESTAMP,
`thread_updated` datetime NULL DEFAULT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY (`thread_id`),
INDEX (`thread_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


CREATE TABLE IF NOT EXISTS `restricted`(
`id` bigint(20) NOT NULL AUTO_INCREMENT,
`author_user_id` bigint(20) NOT NULL,
PRIMARY KEY (id),
INDEX (`author_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
