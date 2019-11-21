package com.paladin.qos.analysis;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;

/**
 * 数据处理器，对数据进行时间维度和机构维度的数据预处理，提高统计效率
 * 
 * @author TontoZhou
 * @since 2019年8月14日
 */
public abstract class DataProcessor {

	private static Logger logger = LoggerFactory.getLogger(DataProcessor.class);

	private DataEvent dataEvent;
	private List<DataUnit> targetUnits;
	
	
	/**
	 * 处理器处理的事件ID
	 * 
	 * @return
	 */
	public abstract String getEventId();
	
	/**
	 * 设置事件配置
	 * 
	 * @param dataEvent
	 */
	public void setDataEvent(DataEvent dataEvent) {
		this.dataEvent = dataEvent;
	}

	/**
	 * 获取事件配置
	 * 
	 * @return
	 */
	public DataEvent getDataEvent() {
		return dataEvent;
	}
	
	/**
	 * 获取目标单位
	 * 
	 * @return
	 */
	public List<DataUnit> getTargetUnits() {
		return targetUnits;
	}

	/**
	 * 设置目标单位
	 * 
	 * @param targetUnits
	 */
	public void setTargetUnits(List<DataUnit> targetUnits) {
		this.targetUnits = targetUnits;
	}

	/**
	 * 处理数据，开始结束时间应当已经处理过（按照timeType），例如开始时间2019-8-13 00:00:00，结束时间为2019-8-14
	 * 00:00:00，不能带时分秒
	 * 
	 * @param startTime
	 * @param endTime
	 * @param unitId
	 * @return 如果返回null表示不处理
	 */
	public Metadata processByDay(Date startTime, Date endTime, String unitId) {

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

		long begin = System.currentTimeMillis();

		long totalNum = getTotalNum(startTime, endTime, unitId);
		long eventNum = getEventNum(startTime, endTime, unitId);

		long expend = System.currentTimeMillis() - begin;
		if (expend > 15 * 1000) {
			logger.warn("处理数据耗时过长！耗时：" + expend + "ms，事件：" + getEventId());
		}

		Metadata metadata = new Metadata();
		metadata.setEventNum(eventNum);
		metadata.setTotalNum(totalNum);
		metadata.setUnitValue(unitId);
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





}
