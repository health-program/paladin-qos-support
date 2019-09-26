package com.paladin.qos.analysis;

public class RateMetadata {
	
	private long totalNum;
	private long eventNum;
	
	// 时间维度
	private int timeType;
	private int year;
	private int month;
	private int day;
	private int weekYear;
	private int weekMonth;
		
	// 机构、群体维度
	private String unitValue;
	
	private String eventId;

	public int getTimeType() {
		return timeType;
	}

	public void setTimeType(int timeType) {
		this.timeType = timeType;
	}

	public String getUnitValue() {
		return unitValue;
	}

	public void setUnitValue(String unitValue) {
		this.unitValue = unitValue;
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

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWeekYear() {
		return weekYear;
	}

	public void setWeekYear(int weekYear) {
		this.weekYear = weekYear;
	}

	public int getWeekMonth() {
		return weekMonth;
	}

	public void setWeekMonth(int weekMonth) {
		this.weekMonth = weekMonth;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	
}
