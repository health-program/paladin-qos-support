package com.paladin.qos.analysis;

public class DataRealTime {

	private String unitId;
	private long totalNum;
	private long eventNum;

	public DataRealTime(String unitId, long totalNum, long eventNum) {
		this.unitId = unitId;
		this.totalNum = totalNum;
		this.eventNum = eventNum;
	}

	public String getUnitId() {
		return unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}

	public long getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(long totalNum) {
		this.totalNum = totalNum;
	}

	public long getEventNum() {
		return eventNum;
	}

	public void setEventNum(long eventNum) {
		this.eventNum = eventNum;
	}
}
