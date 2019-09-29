package com.paladin.qos.controller.analysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class AnalysisRequest {

	public static Date DEFAULT_START_TIME;

	static {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			DEFAULT_START_TIME = format.parse("2019-01-01");
		} catch (ParseException e) {
		}
	}

	private Date startTime = DEFAULT_START_TIME;
	private Date endTime = new Date();
	private String unitId;
	private String eventId;

	private List<String> unitIds;
	private List<String> eventIds;

	private int dataType;
	private int byUnit = 1;

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime == null ? DEFAULT_START_TIME : startTime;
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

	public int getByUnit() {
		return byUnit;
	}

	public void setByUnit(int byUnit) {
		this.byUnit = byUnit;
	}

}
