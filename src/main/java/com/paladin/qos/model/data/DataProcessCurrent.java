package com.paladin.qos.model.data;

import java.util.Date;
import javax.persistence.Id;

public class DataProcessCurrent {

	// 
	@Id
	private String eventId;
	
	@Id
	private String unitId;

	// 
	private Date currentDay;

	// 
	private Date updateTime;

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public Date getCurrentDay() {
		return currentDay;
	}

	public void setCurrentDay(Date currentDay) {
		this.currentDay = currentDay;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public String getUnitId() {
		return unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}

}