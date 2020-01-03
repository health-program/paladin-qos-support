package com.paladin.qos.dynamic.mapper.yiyuan.score.lianxuxing;

import org.apache.ibatis.annotations.Param;

import java.util.Date;

/**
 * <p>功能描述</p>：
 *
 * @author Huangguochen
 * @create 2019/12/30 14:58
 */
public interface HospitalContinuityScoreMapper {

    long getMedicalRecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    long getRegisterRecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    long getTcmPrescriptionTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    long getWesternPrescriptionTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    long getExaminationrecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    long getTestrecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    long getCostrecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

}
