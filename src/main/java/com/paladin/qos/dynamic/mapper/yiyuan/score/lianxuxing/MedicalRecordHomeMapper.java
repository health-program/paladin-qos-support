package com.paladin.qos.dynamic.mapper.yiyuan.score.lianxuxing;

import org.apache.ibatis.annotations.Param;

import java.util.Date;

/**
 * <p>功能描述</p>：
 *
 * @author Huangguochen
 * @create 2019/12/30 14:58
 */
public interface MedicalRecordHomeMapper {

    long getTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
}
