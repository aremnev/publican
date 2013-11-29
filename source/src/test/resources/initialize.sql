DROP TABLE IF EXISTS `common`;
CREATE TABLE `common` (
  `id` bigint(20) unsigned NOT NULL,
  `text` varchar(255) NOT NULL,
  `date` TIMESTAMP NOT NULL DEFAULT '2011-12-31 23:59:59',
  PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `sharded`;
CREATE TABLE `sharded` (
  `id` bigint(20) unsigned NOT NULL,
  `text` varchar(255) NOT NULL,
  `date` TIMESTAMP NOT NULL DEFAULT '2011-12-31 23:59:59',
  PRIMARY KEY (`id`)
);