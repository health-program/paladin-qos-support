package com.paladin.qos.dynamic.mapper.gongwei;

import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;

public interface PublicHealthManagementMapper {

    //活动档案数
    Long getActiveArchives(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //审核公开的健康档案数
    Long getPublicArchives(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //老年人体检数
    Long getPhysicalNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //同辖区老人人数
    Long getOldPeopleNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //最近一次血压达标人数
    Long getRecentPressureReachNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //高血压人数
    Long getPressureNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //高血压管理规范管理数
   Long getPressureManageNumber(@Param("yearList")List<String> yearList, @Param("unitId") String unitId);

    //高血压管理随访数
    Long getPressureFollowNumber(@Param("yearList")List<String> yearList, @Param("unitId") String unitId);

    //糖尿病患病数
    Long getSugarNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //最近一次血糖达标人数
    Long getRecentSugarReachNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //高血糖管理规范管理数
    Long getSugarManageNumber(@Param("yearList")List<String> yearList, @Param("unitId") String unitId);

    //高血糖管理随访数
    Long getSugarFollowNumber(@Param("yearList")List<String> yearList, @Param("unitId") String unitId);

    //有效转诊的可疑症状的患者数
    Long getReferralNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //糖尿病患者和老年人可疑症状患者数
    Long getPatientNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);

    //辖区内对65岁以上老年人规范进行中医体质辨识和中医保健指导人数
    Long getGuideNumber(@Param("startDate") Date startDate, @Param("endDate") Date endDate, @Param("unitId") String unitId);
}
