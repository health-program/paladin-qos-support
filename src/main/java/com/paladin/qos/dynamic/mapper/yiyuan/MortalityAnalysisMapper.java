package com.paladin.qos.dynamic.mapper.yiyuan;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

public interface MortalityAnalysisMapper {

	/**
	 * 时间段内医院住院死亡人数
	 * @param startTime
	 * @param endTime
	 * @param unitId
	 * @return
	 */
	public long getFatalitiesInHospital(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/**
	 * 时间段内医院出院人数
	 * @param startTime
	 * @param endTime
	 * @param unitId
	 * @return
	 */
	public long getNumberOutHospital(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	
}
