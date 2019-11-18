package com.paladin.qos.dynamic.mapper.yiyuan;

import org.apache.ibatis.annotations.Param;

import java.util.Date;

/**
 * <p>功能描述</p>：
 *
 * @author Huangguochen
 * @create 2019/11/18 17:17
 */
public interface EssentialDrugAnalysisMapper {

    long getTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getEventNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

}
