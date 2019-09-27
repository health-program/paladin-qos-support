package com.paladin.qos.mapper.analysis;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.paladin.qos.service.analysis.data.AnalysisUnit;
import com.paladin.qos.service.analysis.data.DataCountDay;
import com.paladin.qos.service.analysis.data.DataCountMonth;
import com.paladin.qos.service.analysis.data.DataCountUnit;
import com.paladin.qos.service.analysis.data.DataCountYear;
import com.paladin.qos.service.analysis.data.DataPointDay;
import com.paladin.qos.service.analysis.data.DataPointMonth;
import com.paladin.qos.service.analysis.data.DataPointWeekMonth;
import com.paladin.qos.service.analysis.data.DataPointWeekYear;
import com.paladin.qos.service.analysis.data.DataPointYear;

public interface AnalysisMapper {

	// 日节点数据集合
	List<DataPointDay> getDataPointOfDayByUnit(@Param("eventId") String eventId, @Param("unitId") String unitId, @Param("start") int start,
			@Param("end") int end);

	List<DataPointDay> getDataPointOfDay(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start, @Param("end") int end);

	// 月节点数据集合
	List<DataPointMonth> getDataPointOfMonthByUnit(@Param("eventId") String eventId, @Param("unitId") String unitId, @Param("start") int start,
			@Param("end") int end);

	List<DataPointMonth> getDataPointOfMonth(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	// 年节点数据集合
	List<DataPointYear> getDataPointOfYearByUnit(@Param("eventId") String eventId, @Param("unitId") String unitId, @Param("start") int start,
			@Param("end") int end);

	List<DataPointYear> getDataPointOfYear(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start, @Param("end") int end);

	// 年中星期节点数据集合
	List<DataPointWeekYear> getDataPointOfWeekYearByUnit(@Param("eventId") String eventId, @Param("unitId") String unitId, @Param("start") int start,
			@Param("end") int end);

	List<DataPointWeekYear> getDataPointOfWeekYear(@Param("eventId") String eventId, @Param("start") int start, @Param("end") int end);

	// 月中星期节点数据集合
	List<DataPointWeekMonth> getDataPointOfWeekMonthByUnit(@Param("eventId") String eventId, @Param("unitId") String unitId, @Param("start") int start,
			@Param("end") int end);

	List<DataPointWeekMonth> getDataPointOfWeekMonth(@Param("eventId") String eventId, @Param("start") int start, @Param("end") int end);

	// 按单位分组查询总数据
	List<AnalysisUnit> getAnalysisResultGroupByUnit(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	List<DataCountUnit> countTotalNumByUnit(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	List<DataCountUnit> countEventNumByUnit(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	List<DataCountDay> countTotalNumByDay(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start, @Param("end") int end);

	List<DataCountDay> countEventNumByDay(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start, @Param("end") int end);

	List<DataCountMonth> countTotalNumByMonth(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	List<DataCountMonth> countEventNumByMonth(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	List<DataCountYear> countTotalNumByYear(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	List<DataCountYear> countEventNumByYear(@Param("eventId") String eventId, @Param("unitType") int unitType, @Param("start") int start,
			@Param("end") int end);

	List<Integer> getSerialNumbers(@Param("eventId") String eventId, @Param("unitId") String unitId);

	Integer getMaxSerialNumByEventAndUnit(@Param("eventId") String eventId, @Param("unitId") String unitId);

	int removeDataOfDay(@Param("serialNumber") int serialNumber, @Param("eventId") String eventId);

	long getTotalNumOfEvent(@Param("eventId") String eventId, @Param("start") int start, @Param("end") int end);

	List<DataCountUnit> getLastCountByUnit(@Param("eventId") String eventId, @Param("unitType") int unitType);

}
