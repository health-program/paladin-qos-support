package com.paladin.qos.model.data;

import java.util.Date;

import javax.persistence.Id;

import com.paladin.qos.core.DataTaskConfiguration;

public class DataEvent extends DataTaskConfiguration {

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

	// 内容说明
	private String content;

	// 处理开始时间
	private Date processStartDate;

	// 最大迁移条数
	private Integer maximumProcess;

	// 是否单独处理线程
	private Integer separateProcessThread;

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

	public Integer getEventType() {
		return eventType;
	}

	public void setEventType(Integer eventType) {
		this.eventType = eventType;
	}

	public Integer getTargetType() {
		return targetType;
	}

	public void setTargetType(Integer targetType) {
		this.targetType = targetType;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Date getProcessStartDate() {
		return processStartDate;
	}

	public void setProcessStartDate(Date processStartDate) {
		this.processStartDate = processStartDate;
	}

	public Integer getSeparateProcessThread() {
		return separateProcessThread;
	}

	public void setSeparateProcessThread(Integer separateProcessThread) {
		this.separateProcessThread = separateProcessThread;
	}

	public Integer getMaximumProcess() {
		return maximumProcess;
	}

	public void setMaximumProcess(Integer maximumProcess) {
		this.maximumProcess = maximumProcess;
	}

}