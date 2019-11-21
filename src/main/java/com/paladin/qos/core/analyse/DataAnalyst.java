package com.paladin.qos.core.analyse;

import java.util.Date;
import java.util.List;

import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;

public abstract class DataAnalyst {

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
	 * 获取时间段内某机构的数据结果
	 * 
	 * @param startTime
	 * @param endTime
	 * @param unitId
	 * @return
	 */
	public abstract String getDataResult(Date startTime, Date endTime, String unitId);


}
