package com.paladin.qos.model.data;

import java.util.Date;

import javax.persistence.Id;

public class DataProcessedDay {
	
	public static final String COLUMN_FIELD_EVENT_ID = "eventId";
	public static final String COLUMN_FIELD_UNIT_ID = "unitId";
	
	public static final String COLUMN_FIELD_YEAR = "year";
	public static final String COLUMN_FIELD_MONTH = "month";
	public static final String COLUMN_FIELD_DAY = "day";
	
	// ID
	@Id
	private String id;

	// 事件ID
	private String eventId;

	// 年份
	private Integer year;

	// 月份
	private Integer month;

	// 日期
	private Integer day;

	// 按年第几个星期
	private Integer weekYear;

	// 按月第几个星期
	private Integer weekMonth;

	// 顺序号
	private Integer serialNumber;
	
	// 机构单位ID
	private String unitId;
	
	// 机构类型
	private Integer unitType;

	// 样本总数
	private Long totalNum;

	// 事件总数
	private Long eventNum;

	// 比率
	private Integer rate;
	
	// 更新时间
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

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public Integer getDay() {
		return day;
	}

	public void setDay(Integer day) {
		this.day = day;
	}

	public Integer getWeekYear() {
		return weekYear;
	}

	public void setWeekYear(Integer weekYear) {
		this.weekYear = weekYear;
	}

	public Integer getWeekMonth() {
		return weekMonth;
	}

	public void setWeekMonth(Integer weekMonth) {
		this.weekMonth = weekMonth;
	}

	public String getUnitId() {
		return unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}

	public Long getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(Long totalNum) {
		this.totalNum = totalNum;
	}

	public Long getEventNum() {
		return eventNum;
	}

	public void setEventNum(Long eventNum) {
		this.eventNum = eventNum;
	}

	public Integer getRate() {
		return rate;
	}

	public void setRate(Integer rate) {
		this.rate = rate;
	}

	public Integer getSerialNumber() {
		return serialNumber;
	}

	public void setSerialNumber(Integer serialNumber) {
		this.serialNumber = serialNumber;
	}

	public Integer getUnitType() {
		return unitType;
	}

	public void setUnitType(Integer unitType) {
		this.unitType = unitType;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}


}