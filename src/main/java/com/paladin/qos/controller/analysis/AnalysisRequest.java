package com.paladin.qos.controller.analysis;

import java.util.Date;
import java.util.List;

import com.paladin.qos.analysis.DataProcessManager;

public class AnalysisRequest {

	private Date startTime = DataProcessManager.DEFAULT_START_TIME;
	private Date endTime = new Date();
	private String unitId;
	private String eventId;

	private List<String> unitIds;
	private List<String> eventIds;

	private int dataType;

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime == null ? DataProcessManager.DEFAULT_START_TIME : startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime == null ? new Date() : endTime;
	}

	public List<String> getUnitIds() {
		return unitIds;
	}

	public void setUnitIds(List<String> unitIds) {
		this.unitIds = unitIds;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public List<String> getEventIds() {
		return eventIds;
	}

	public void setEventIds(List<String> eventIds) {
		this.eventIds = eventIds;
	}

	public String getUnitId() {
		return unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

}
