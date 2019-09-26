package com.paladin.qos.dynamic.mapper.exhibition;

import org.apache.ibatis.annotations.Param;

import java.util.Date;

/**
 * <p>功能描述</p>：计划生育管理
 *
 * @author Huangguochen
 * @create 2019/9/3 18:00
 */
public interface FamilyPlanningManagementMapper {

    long getCondomDistributionTotal(@Param("date") Date date);

    long getBirthControlPillsTotal(@Param("date") Date date);

    long getMedicalAbortionTotal(@Param("date") Date date);

    long getNegativePressureSuctionTotal(@Param("date") Date date);

    long getIntrauterineDevicePlacementTotal(@Param("date") Date date);

    long getIntrauterineDeviceRemovalTotal(@Param("date") Date date);

    long getCondomDistributionNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getBirthControlPillsNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getMedicalAbortionNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getNegativePressureSuctionNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getIntrauterineDevicePlacementNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getIntrauterineDeviceRemovalNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);


}
