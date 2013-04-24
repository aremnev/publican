-- DROP TABLE IF EXISTS `users`;
-- CREATE TABLE `users` (
--  `id` bigint(20) unsigned NOT NULL,
--  `email` varchar(255) NOT NULL,
--  `name` varchar(255) NOT NULL,
--  `birthDate` TIMESTAMP NOT NULL,
--  PRIMARY KEY (`id`)
-- );

-- INSERT INTO `users` VALUES
-- (1, 'user1@gmail.com', 'user1', '1980-01-01 00:00:00'),
-- (2, 'user2@gmail.com', 'user2', '1980-01-02 00:00:00'),
-- (3, 'user3@gmail.com', 'user3', '1980-01-03 00:00:00'),
-- (4, 'user4@gmail.com', 'user4', '1980-01-04 00:00:00'),
-- (5, 'user5@gmail.com', 'user5', '1980-01-05 00:00:00'),
-- (6, 'user6@gmail.com', 'user6', '1980-01-06 00:00:00'),
-- (7, 'user7@gmail.com', 'user7', '1980-01-07 00:00:00'),
-- (8, 'user8@gmail.com', 'user8', '1980-01-08 00:00:00'),
-- (9, 'user9@gmail.com', 'user9', '1980-01-09 00:00:00'),
-- (10, 'user10@gmail.com', 'user10', '1980-01-10 00:00:00'),
-- (11, 'user11@gmail.com', 'user11', '1980-01-11 00:00:00'),
-- (12, 'user12@gmail.com', 'user12', '1980-01-12 00:00:00'),
-- (13, 'user13@gmail.com', 'user13', '1980-01-13 00:00:00'),
-- (14, 'user14@gmail.com', 'user14', '1980-01-14 00:00:00'),
-- (15, 'user15@gmail.com', 'user15', '1980-01-15 00:00:00');

DROP TABLE IF EXISTS `friends`;
CREATE TABLE `friends` (
  `userId` bigint(20) unsigned NOT NULL,
  `friendId` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`userId`, `friendId`),
--  FOREIGN KEY(`userId`) REFERENCES `users`(`id`) ON DELETE CASCADE
);

-- INSERT INTO `friends` VALUES
-- (1, 2),
-- (1, 3),
-- (1, 4),
-- (1, 5),
-- (1, 6),
-- (1, 7),
-- (1, 8),
-- (1, 15),
-- (2, 1),
-- (2, 3),
-- (2, 4),
-- (2, 5),
-- (2, 6),
-- (2, 7),
-- (2, 8),
-- (2, 9),
-- (2, 15),
-- (3, 1),
-- (3, 2),
-- (3, 4),
-- (3, 5),
-- (3, 6),
-- (3, 7),
-- (3, 8),
-- (3, 9),
-- (3, 10),
-- (3, 11),
-- (4, 1),
-- (4, 2),
-- (4, 3),
-- (4, 5),
-- (4, 6),
-- (4, 7),
-- (4, 8),
-- (4, 9),
-- (4, 10),
-- (4, 11),
-- (4, 12),
-- (5, 1),
-- (5, 2),
-- (5, 3),
-- (5, 4),
-- (5, 6),
-- (6, 1),
-- (6, 2),
-- (6, 3),
-- (6, 4),
-- (6, 5),
-- (7, 1),
-- (7, 2),
-- (7, 3),
-- (7, 4),
-- (8, 1),
-- (8, 2),
-- (8, 3),
-- (8, 4),
-- (9, 2),
-- (9, 3),
-- (9, 4),
-- (10, 3),
-- (10, 4),
-- (11, 3),
-- (11, 4),
-- (12, 4),
-- (13, 14),
-- (14, 13),
-- (15, 1),
-- (15, 2);
