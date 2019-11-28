package com.paladin.qos.dynamic.mapper.yiyuan;

import org.apache.ibatis.annotations.Param;

import java.util.Date;

/**
 * <p>功能描述</p>：昆山市公立医院绩效考核表
 *
 * @author Huangguochen
 * @create 2019/11/28 10:56
 */
public interface HospitalPerformanceAppraisalMapper {

    long getOutpatientVisitsTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getReferralsTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getOperationsTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long get34OperationsTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getOperationsTablesTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getOperationsInfectionTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getEnrollmentTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

}
