package com.paladin.qos.service.analysis.data;

import com.paladin.qos.analysis.DataConstantContainer;

public class TestResult {

	private String eventId;
	private long castTime; // 花费时间
	private int processTimes; // 花费次数
	private boolean isSuccess;
	private String exception;

	public String getEventName() {
		return DataConstantContainer.getEventName(eventId);
	}
	
	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public long getCastTime() {
		return castTime;
	}

	public void setCastTime(long castTime) {
		this.castTime = castTime;
	}

	public boolean isSuccess() {
		return isSuccess;
	}

	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}

	public String getException() {
		return exception;
	}

	public void setException(String exception) {
		this.exception = exception;
	}

	public int getProcessTimes() {
		return processTimes;
	}

	public void setProcessTimes(int processTimes) {
		this.processTimes = processTimes;
	}

}
