package com.paladin.qos.model.data;

import java.util.Date;

import javax.persistence.Id;

public class DataAnalytical {
	
	@Id
	private String id;
	
	private String eventId;
	
	private String unitId;
	
	private Integer serialNumber;
	
	private String dataResult;
	
	private Date updateTime;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getUnitId() {
		return unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}

	public Integer getSerialNumber() {
		return serialNumber;
	}

	public void setSerialNumber(Integer serialNumber) {
		this.serialNumber = serialNumber;
	}

	public String getDataResult() {
		return dataResult;
	}

	public void setDataResult(String dataResult) {
		this.dataResult = dataResult;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	
	
	
}
