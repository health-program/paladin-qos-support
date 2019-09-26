package com.paladin.qos.dynamic.mapper.exhibition;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

/**
 * <p>功能描述</p>：儿童保健管理
 *
 * @author Huangguochen
 * @create 2019/9/3 13:25
 */
public interface ChildCareManagementMapper {

    long getInfantDeathTotal(@Param("date") Date date);

    long getNeonatalBirthDefectsTotal(@Param("date") Date date);

    long getNeonatalDiseaseScreeningTotal(@Param("date") Date date);

    long getChildCardTotal(@Param("date") Date date);

    long getFemaleNewbornChildbirthTotal(@Param("date") Date date);

    long getMaleNewbornChildbirthTotal(@Param("date") Date date);

    long getChildHealthCheckTotal(@Param("date") Date date);

    long getNewbornHearingTotal(@Param("date") Date date);

    long getInfantDeathNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getNeonatalBirthDefectsNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getNeonatalDiseaseScreeningNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getChildCardNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getFemaleNewbornChildbirthNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getMaleNewbornChildbirthNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getChildHealthCheckNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getInfantCongenitalHeartDiseaseNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getChildHealthCheckupNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getInfantVisionNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getNewbornHearingNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);
}
