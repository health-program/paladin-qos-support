/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50537
Source Host           : localhost:3306
Source Database       : qos

Target Server Type    : MYSQL
Target Server Version : 50537
File Encoding         : 65001

Date: 2019-11-05 21:04:36
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for data_migration
-- ----------------------------
DROP TABLE IF EXISTS `data_migration`;
CREATE TABLE `data_migration` (
  `id` varchar(20) NOT NULL,
  `name` varchar(50) NOT NULL COMMENT '名称',
  `type` tinyint(4) NOT NULL COMMENT '迁移类型：1：增量更新，2：全部更新',
  `origin_data_source` varchar(30) NOT NULL COMMENT '原始数据源',
  `origin_data_source_type` varchar(20) NOT NULL COMMENT '原始数据源类型',
  `origin_table_name` varchar(50) NOT NULL COMMENT '原始数据源表',
  `target_data_source` varchar(30) NOT NULL COMMENT '目标数据源',
  `target_table_name` varchar(50) NOT NULL COMMENT '目标数据源表',
  `select_data_limit` int(11) NOT NULL COMMENT '每次查询条数限制',
  `maximum_migrate` int(11) NOT NULL COMMENT '最大迁移条数',
  `update_time_field` varchar(50) DEFAULT NULL COMMENT '更新时间字段',
  `primary_key_field` varchar(100) DEFAULT NULL COMMENT '主键字段，可多个，逗号分隔',
  `select_columns` varchar(500) DEFAULT NULL,
  `create_time_field` varchar(50) DEFAULT NULL,
  `update_time_nullable` tinyint(4) NOT NULL DEFAULT '0',
  `default_start_date` date NOT NULL COMMENT '缺省开始处理日期',
  `filing_strategy` tinyint(4) NOT NULL COMMENT '归档策略',
  `filing_strategy_param1` int(11) DEFAULT NULL COMMENT '归档策略参数1，配合数据归档策略用',
  `filing_strategy_param2` varchar(500) DEFAULT NULL COMMENT '归档策略参数2，配合数据归档策略用',
  `schedule_strategy` tinyint(4) NOT NULL,
  `schedule_strategy_param1` int(11) DEFAULT NULL,
  `schedule_strategy_param2` varchar(500) DEFAULT NULL,
  `schedule_hour` int(11) NOT NULL DEFAULT '19',
  `execute_hours` int(11) NOT NULL DEFAULT '10',
  `real_time_enabled` tinyint(4) NOT NULL DEFAULT '1' COMMENT '是否需要实时',
  `real_time_interval` smallint(6) NOT NULL DEFAULT '5' COMMENT '实时间隔，分钟',
  `separate_process_thread` tinyint(4) NOT NULL COMMENT '单独处理线程',
  `millisecond_enabled` tinyint(4) NOT NULL,
  `notes` varchar(500) DEFAULT NULL COMMENT '备注',
  `enabled` tinyint(4) NOT NULL COMMENT '是否启用',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of data_migration
-- ----------------------------
INSERT INTO `data_migration` VALUES ('M200001', '高血压', '11', 'gongwei', 'oracle', 'phis_chss.v_report_year_blood', 'local', 'report_year_blood', '500', '10000', 'REPORTYEAR', 'ID', '*', null, '0', '2015-01-01', '1', null, null, '3', null, '2/10,2/20,3/1,3/10', '19', '10', '0', '5', '0', '0', '高血压上报', '1');
INSERT INTO `data_migration` VALUES ('M200002', '糖尿病', '11', 'gongwei', 'oracle', 'phis_chss.v_report_year_diabetes', 'local', 'report_year_diabetes', '500', '10000', 'REPORTYEAR', 'ID', '*', null, '0', '2015-01-01', '1', null, null, '3', null, '2/10,2/20,3/1,3/10', '19', '10', '0', '5', '0', '0', '糖尿病上报', '1');
INSERT INTO `data_migration` VALUES ('M200010', '家庭医生签约主表', '10', 'gongwei', 'oracle', 'phis_chss.v_fha_person_sign_main', 'local', 'g_fha_person_sign_main', '500', '10000', 'OPERTIME', 'CHID', '*', 'CREATEDTIME', '1', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '10', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200011', '家庭医生签约档案主表', '10', 'gongwei', 'oracle', 'phis_chss.v_fha_person_sign_main_org', 'local', 'g_fha_person_sign_main_org', '500', '10000', 'OPERTIME', 'CHID', '*', 'CREATEDTIME', '1', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '10', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200012', '家庭医生随访表', '10', 'gongwei', 'oracle', 'phis_chss.v_fha_person_sign_visit', 'local', 'g_fha_person_sign_visit', '500', '10000', 'OPERTIME', 'CHID', '*', 'CREATEDTIME', '1', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '10', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200013', '家庭医生随访临时表', '10', 'gongwei', 'oracle', 'phis_chss.v_fha_person_sign_visit_temp', 'local', 'g_fha_person_sign_visit_temp', '500', '10000', 'OPERTIME', 'CHID', '*', '', '0', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '10', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200014', '家庭医生检副本1', '10', 'gongwei', 'oracle', 'phis_chss.v_fhc_check_attach1', 'local', 'g_fhc_check_attach1', '500', '10000', 'OPERTIME', 'CHID', '*', '', '0', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '10', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200015', '家庭医生体检主表', '10', 'gongwei', 'oracle', 'phis_chss.v_fhc_check_main', 'local', 'g_fhc_check_main', '500', '10000', 'OPERTIME', 'CHID', '*', '', '0', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '12', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200016', '家庭医生糖尿病随访表', '10', 'gongwei', 'oracle', 'phis_chss.v_fhd_diabetes_visit', 'local', 'g_fhd_diabetes_visit', '500', '10000', 'OPERTIME', 'CHID', '*', '', '0', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '12', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200017', '家庭医生老年人自理评估表', '10', 'gongwei', 'oracle', 'phis_chss.v_fhr_elder_self_care', 'local', 'g_fhr_elder_self_care', '500', '10000', 'OPERTIME', 'CHID', '*', 'CREATEDTIME', '1', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '12', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200018', '家庭医生老年人中医评估表', '10', 'gongwei', 'oracle', 'phis_chss.v_fhr_elder_tcm_item', 'local', 'g_fhr_elder_tcm_item', '500', '10000', 'OPERTIME', 'CHID', '*', '', '0', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '12', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200019', '家庭医生老年人中医体质评估表', '10', 'gongwei', 'oracle', 'phis_chss.v_fhr_elder_tcm_physique', 'local', 'g_fhr_elder_tcm_physique', '500', '10000', 'OPERTIME', 'CHID', '*', '', '0', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '12', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200020', '家庭医生建档人员表', '10', 'gongwei', 'oracle', 'phis_chss.v_fha_person_main', 'local', 'g_fha_person_main', '500', '10000', 'OPERTIME', 'CHID', '*', '', '0', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '10', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M200021', '脑卒中评估', '10', 'gongwei', 'oracle', 'phis_chss.v_fha_pre_scale_assess', 'local', 'g_fha_pre_scale_assess', '500', '10000', 'OPERTIME', 'CHID', '*', 'CREATEDTIME', '1', '2015-01-01', '1', null, null, '1', null, null, '19', '10', '1', '10', '0', '1', '', '1');
INSERT INTO `data_migration` VALUES ('M300001', '门诊挂号表', '10', 'jcyl', 'oracle', 'phis_webhis.v_opr_register', 'local', 'j_opr_register', '500', '10000', 'REGDATE', 'REGID', '*', null, '0', '2019-10-20', '1', null, null, '1', null, null, '19', '10', '1', '5', '0', '0', null, '1');
