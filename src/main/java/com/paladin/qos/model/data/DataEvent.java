package com.paladin.qos.model.data;

import javax.persistence.Id;

public class DataEvent {

	public static final int EVENT_TYPE_RATE = 1;
	public static final int EVENT_TYPE_COUNT = 2;
	
	public static final int TARGET_TYPE_ALL = 1;
	public static final int TARGET_TYPE_HOSPITAL = 2;
	public static final int TARGET_TYPE_COMMUNITY = 3;
	
	// 
	@Id
	private String id;

	// 事件名称
	private String name;
	
	// 事件类型，概率 or 总数
	private Integer eventType;
	
	// 数据目标类型， 医院 or 社区 or 所有
	private Integer targetType;

	// 数据源
	private String dataSource;
	
	// 
	private String content;
	
	// 处理前多少天数据
	private Integer processBefore;
	
	// 是否需要实时
	private Integer realTimeEnabled;
	
	// 实时间隔时间，分钟
	private Integer realTimeInterval;

	// 是否启用
	private Integer enabled;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Integer getEnabled() {
		return enabled;
	}

	public void setEnabled(Integer enabled) {
		this.enabled = enabled;
	}

	public Integer getTargetType() {
		return targetType;
	}

	public void setTargetType(Integer targetType) {
		this.targetType = targetType;
	}

	public Integer getEventType() {
		return eventType;
	}

	public void setEventType(Integer eventType) {
		this.eventType = eventType;
	}
	
	public Integer getRealTimeInterval() {
		return realTimeInterval;
	}

	public void setRealTimeInterval(Integer realTimeInterval) {
		this.realTimeInterval = realTimeInterval;
	}

	public Integer getRealTimeEnabled() {
		return realTimeEnabled;
	}

	public void setRealTimeEnabled(Integer realTimeEnabled) {
		this.realTimeEnabled = realTimeEnabled;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public Integer getProcessBefore() {
		return processBefore;
	}

	public void setProcessBefore(Integer processBefore) {
		this.processBefore = processBefore;
	}

}