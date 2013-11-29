
DROP TABLE IF EXISTS `friends`;
CREATE TABLE `friends` (
  `userId` bigint(20) unsigned NOT NULL,
  `friendId` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`userId`, `friendId`)
);
