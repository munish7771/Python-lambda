drop table tbl_user;
CREATE TABLE tbl_user (
  `user_id` bigint(20) Primary key AUTO_INCREMENT,
  `user_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_email` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_password` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL
)
select * from tbl_user;