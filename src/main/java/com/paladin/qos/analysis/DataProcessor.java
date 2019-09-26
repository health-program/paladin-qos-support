package com.paladin.qos.analysis;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.paladin.qos.analysis.DataConstantContainer.Unit;

/**
 * 数据处理器，对数据进行时间维度和机构维度的数据预处理，提高统计效率
 * 
 * @author TontoZhou
 * @since 2019年8月14日
 */
public abstract class DataProcessor {

	/** 时间粒度：按年 */
	public final static int TIME_GRANULARITY_YEAR = 10;
	/** 时间粒度：按月 */
	public final static int TIME_GRANULARITY_MONTH = 11;
	/** 时间粒度：按日 */
	public final static int TIME_GRANULARITY_DAY = 12;

	/**
	 * 处理器处理的事件ID
	 * 
	 * @return
	 */
	public abstract String getEventId();

	/**
	 * 处理时间粒度
	 * 
	 * @return
	 */
	// public int getTimeGranularity() {
	// return TIME_GRANULARITY_DAY;
	// }

	/**
	 * 处理数据，开始结束时间应当已经处理过（按照timeType），例如开始时间2019-8-13 00:00:00，结束时间为2019-8-14
	 * 00:00:00，不能带时分秒
	 * 
	 * @param timeType
	 * @param startTime
	 * @param endTime
	 * @param unitId
	 * @return 如果返回null表示不处理
	 */
	public RateMetadata processByDay(Date startTime, Date endTime, String unitId) {

		if (unitId == null || unitId.length() == 0) {
			return null;
		}

		Calendar c = Calendar.getInstance();
		c.setTime(startTime);

		int year = c.get(Calendar.YEAR);
		int month = c.get(Calendar.MONTH) + 1;
		int day = c.get(Calendar.DAY_OF_MONTH);
		int weekYear = c.get(Calendar.WEEK_OF_YEAR);
		int weekMonth = c.get(Calendar.WEEK_OF_MONTH);

		long totalNum = getTotalNum(startTime, endTime, unitId);
		long eventNum = getEventNum(startTime, endTime, unitId);

		RateMetadata metadata = new RateMetadata();
		metadata.setEventNum(eventNum);
		metadata.setTotalNum(totalNum);
		metadata.setUnitValue(unitId);
		metadata.setTimeType(DataProcessor.TIME_GRANULARITY_DAY);
		metadata.setYear(year);
		metadata.setMonth(month);
		metadata.setDay(day);
		metadata.setWeekMonth(weekMonth);
		metadata.setWeekYear(weekYear);
		metadata.setEventId(getEventId());

		return metadata;
	}

	/**
	 * 获取时间段内某机构的统计样本总数
	 * 
	 * @param startTime
	 * @param endTime
	 * @param unitId
	 * @return
	 */
	public abstract long getTotalNum(Date startTime, Date endTime, String unitId);

	/**
	 * 获取时间段内某机构的统计事件总数
	 * 
	 * @param startTime
	 * @param endTime
	 * @param unitId
	 * @return
	 */
	public abstract long getEventNum(Date startTime, Date endTime, String unitId);

	/**
	 * 返回实时数据时间区间
	 * 
	 * @param endTime
	 * @return
	 */
	public Date[] getDataRealTimeInterval() {
		Date today = TimeUtil.toDayTime(new Date());
		Date endTime = new Date(today.getTime() + TimeUtil.MILLIS_IN_DAY);
		return new Date[] { today, endTime };
	}

	/**
	 * 获取实时数据
	 * 
	 * @param unitIds
	 * @return
	 */
	public Map<String, DataRealTime> getDataRealTime(List<Unit> units) {
		if (units == null || units.size() == 0) {
			return null;
		}

		Date[] dates = getDataRealTimeInterval();
		Date startTime = dates[0];
		Date endTime = dates[1];

		HashMap<String, DataRealTime> datas = new HashMap<>();

		for (Unit unit : units) {
			String unitId = unit.getId();
			long totalNum = getTotalNum(startTime, endTime, unitId);
			long eventNum = getEventNum(startTime, endTime, unitId);
			DataRealTime data = new DataRealTime(unitId, totalNum, eventNum);
			datas.put(unitId, data);
		}

		return datas;
	}

}
