package com.paladin.qos.dynamic.mapper.yiyuan.score.guanlianxing;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

public interface GuanlianxingMapper {

	//中医处方表
	long getCMprescriptionsTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	long getCMprescriptionsEventNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	//西医处方表
	long getWMprescriptionsTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	long getWMprescriptionsEventNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	//检查表
	long getExaminationRecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	long getExaminationRecordEventNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	//检验表
	long getTestrecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	long getTestrecordEventNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	//收费表
	long getCostrecordTotalNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	long getCostrecordEventNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	

}
