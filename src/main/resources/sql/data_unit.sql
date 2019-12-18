/*
Navicat MySQL Data Transfer

Source Server         : 10.9.1.42
Source Server Version : 50537
Source Host           : 10.9.1.42:3306
Source Database       : qos

Target Server Type    : MYSQL
Target Server Version : 50537
File Encoding         : 65001

Date: 2019-12-11 09:59:25
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for data_unit
-- ----------------------------
DROP TABLE IF EXISTS `data_unit`;
CREATE TABLE `data_unit` (
  `id` varchar(32) NOT NULL,
  `name` varchar(50) NOT NULL COMMENT '医院名称',
  `type` tinyint(4) NOT NULL,
  `parent_id` varchar(32) DEFAULT NULL,
  `note` varchar(500) NOT NULL COMMENT '备注说明',
  `bed_number` int(11) DEFAULT NULL COMMENT '床位数量',
  `order_num` int(11) NOT NULL,
  `fuyou_code` varchar(20) DEFAULT NULL COMMENT '妇幼对应code',
  `gongwei_code` varchar(20) DEFAULT NULL,
  `db_code` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of data_unit
-- ----------------------------
INSERT INTO `data_unit` VALUES ('320583100467170433', '蓬朗社区卫生服务中心', '2', null, '蓬朗社区卫生服务中心', null, '20', '018315', '320583100700', null);
INSERT INTO `data_unit` VALUES ('320583100PDY002111', '震川社区卫生服务中心', '2', null, '震川社区卫生服务中心', null, '29', '018335', '320583100200', null);
INSERT INTO `data_unit` VALUES ('320583100PDY00212X', '江浦社区卫生服务中心', '2', null, '江浦社区卫生服务中心', null, '27', '018331', '320583100300', null);
INSERT INTO `data_unit` VALUES ('320583100PDY002138', '亭林社区卫生服务中心', '2', null, '亭林社区卫生服务中心', null, '25', '018332', '320583100400', null);
INSERT INTO `data_unit` VALUES ('320583101467170396', '巴城社区卫生服务中心', '2', null, '巴城镇社区卫生服务中心', null, '21', '018310', '320583101400', null);
INSERT INTO `data_unit` VALUES ('320583102467170409', '周市社区卫生服务中心', '2', null, '周市镇社区卫生服务中心', null, '28', '018311', '320583102200', null);
INSERT INTO `data_unit` VALUES ('320583102PDY203693', '柏庐社区卫生服务中心', '2', null, '柏庐社区卫生服务中心', null, '23', '018334', '320583102100', null);
INSERT INTO `data_unit` VALUES ('320583103PDY004045', '陆家社区卫生服务中心', '2', null, '陆家社区卫生服务中心', null, '31', '018306', '320583103100', null);
INSERT INTO `data_unit` VALUES ('320583104PDY000765', '花桥社区卫生服务中心', '2', null, '花桥镇社区卫生服务中心', null, '26', '018308', '320583104300', null);
INSERT INTO `data_unit` VALUES ('320583105467170400', '淀山湖社区卫生服务中心', '2', null, '淀山湖镇社区卫生服务中心', null, '25', '018316', '320583105200', null);
INSERT INTO `data_unit` VALUES ('320583106PDY111254', '张浦社区卫生服务中心', '2', null, '张浦社区卫生服务中心', null, '22', '018307', '320583106400', null);
INSERT INTO `data_unit` VALUES ('320583107467170500', '周庄社区卫生服务中心', '2', null, '周庄镇社区卫生服务中心（医院）', null, '26', '018313', '320583107200', null);
INSERT INTO `data_unit` VALUES ('32058310846717045X', '千灯社区卫生服务中心', '2', null, '千灯镇社区卫生服务中心', null, '24', '018309', '320583108200', null);
INSERT INTO `data_unit` VALUES ('320583109467170300', '锦溪社区卫生服务中心', '2', null, '锦溪社区卫生服务中心', null, '32', '018312', '320583109200', null);
INSERT INTO `data_unit` VALUES ('320583400PDY036125', '青阳社区卫生服务中心', '2', null, '青阳社区卫生服务中心', null, '30', '018333', '320583400100', null);
INSERT INTO `data_unit` VALUES ('320583467170249', '昆山市第一人民医院', '1', null, '昆山市第一人民医院', '1208', '1', '018302', null, 'yiyuan-dyrmyy');
INSERT INTO `data_unit` VALUES ('320583467170257', '昆山市中医医院', '1', null, '昆山市中医医院', '982', '2', '018303', null, 'yiyuan-zyyy');
INSERT INTO `data_unit` VALUES ('320583467170265', '昆山市第三人民医院', '1', null, '昆山市第三人民医院', '430', '4', '018305', null, 'yiyuan-dsrmyy');
INSERT INTO `data_unit` VALUES ('320583467170337', '昆山市锦溪人民医院', '1', null, '昆山市老年医院', '165', '12', '018321', null, 'yiyuan-jxrmyy');
INSERT INTO `data_unit` VALUES ('320583467170345', '昆山市千灯人民医院', '1', null, '昆山市第五人民医院', '206', '13', '018320', null, 'yiyuan-qdrmyy');
INSERT INTO `data_unit` VALUES ('320583467170353', '昆山市精神卫生中心', '1', null, '昆山市巴城人民医院', '320', '8', '018323', null, 'yiyuan-jswszx');
INSERT INTO `data_unit` VALUES ('320583467170361', '昆山市陆家人民医院', '1', null, '昆山市第四人民医院', '200', '6', '018317', null, 'yiyuan-ljrmyy');
INSERT INTO `data_unit` VALUES ('320583467170417', '昆山市周市人民医院', '1', null, '昆山市康复医院', '153', '7', '018322', null, 'yiyuan-zsrmyy');
INSERT INTO `data_unit` VALUES ('320583467170441', '昆山市中西医结合医院', '1', null, '昆山市花桥人民医院', '138', '5', '018319', null, 'yiyuan-zxyjhyy');
INSERT INTO `data_unit` VALUES ('320583467170468', '昆山市淀山湖人民医院', '1', null, '昆山市淀山湖人民医院', '50', '9', '018330', null, 'yiyuan-dshrmyy');
INSERT INTO `data_unit` VALUES ('320583467170476', '昆山市张浦人民医院', '1', null, '昆山市第六人民医院', '266', '10', '018318', null, 'yiyuan-zprmyy');
INSERT INTO `data_unit` VALUES ('320583467170505', '昆山市周庄人民医院', '1', null, '昆山市周庄镇社区卫生服务中心', '40', '11', '018329', null, 'yiyuan-zzrmyy');
INSERT INTO `data_unit` VALUES ('320583467170513', '昆山市第二人民医院', '1', null, '昆山市第二人民医院', '383', '3', '018304', null, 'yiyuan-dermyy');
INSERT INTO `data_unit` VALUES ('320583810343', '昆山市妇幼保健所', '2', null, '妇幼保健所', null, '33', '018301', null, null);
