package com.paladin.qos.dynamic.mapper.yiyuan;

import org.apache.ibatis.annotations.Param;

import java.util.Date;

/**
 * <>
 *
 * @author Huangguochen
 * @create 2019/11/19 10:29
 */
public interface PaymentAnalysisMapper {

    long getTownEmployeeTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    long getCitizensTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    long getNcmsTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    long getBusinessInsuranceTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    long getSelfFeeTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    long getOtherTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
}
