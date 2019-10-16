package com.paladin.qos.controller.task;

import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskConfiguration;

public class DataTaskVO {

	private String id;

	private boolean isRun;
	private long realTimeMillisecond;
	private long threadEndTime;

	// 数据归档策略
	private Integer filingStrategy;
	// 调度任务策略参数1，配合调度任务策略用
	private Integer filingStrategyParam1;
	// 调度任务策略参数2，配合调度任务策略用
	private String filingStrategyParam2;
	// 调度策略
	private Integer scheduleStrategy;
	// 调度任务策略参数1，配合调度任务策略用
	private Integer scheduleStrategyParam1;
	// 调度任务策略参数2，配合调度任务策略用
	private String scheduleStrategyParam2;
	// 是否需要实时
	private Integer realTimeEnabled;
	// 实时间隔，分钟
	private Integer realTimeInterval;
	// 是否启用
	private Integer enabled;

	public DataTaskVO(DataTask task) {
		this.id = task.getId();
		this.isRun = task.isRun();
		this.realTimeMillisecond = task.getRealTimeMillisecond();
		this.threadEndTime = task.getThreadEndTime();

		DataTaskConfiguration config = task.getConfiguration();

		this.filingStrategy = config.getFilingStrategy();
		this.filingStrategyParam1 = config.getFilingStrategyParam1();
		this.filingStrategyParam2 = config.getFilingStrategyParam2();

		this.scheduleStrategy = config.getScheduleStrategy();
		this.scheduleStrategyParam1 = config.getScheduleStrategyParam1();
		this.scheduleStrategyParam2 = config.getScheduleStrategyParam2();

		this.realTimeEnabled = config.getRealTimeEnabled();
		this.realTimeInterval = config.getRealTimeInterval();
		this.enabled = config.getEnabled();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isRun() {
		return isRun;
	}

	public void setRun(boolean isRun) {
		this.isRun = isRun;
	}

	public long getRealTimeMillisecond() {
		return realTimeMillisecond;
	}

	public void setRealTimeMillisecond(long realTimeMillisecond) {
		this.realTimeMillisecond = realTimeMillisecond;
	}

	public long getThreadEndTime() {
		return threadEndTime;
	}

	public void setThreadEndTime(long threadEndTime) {
		this.threadEndTime = threadEndTime;
	}

	public Integer getFilingStrategy() {
		return filingStrategy;
	}

	public void setFilingStrategy(Integer filingStrategy) {
		this.filingStrategy = filingStrategy;
	}

	public Integer getFilingStrategyParam1() {
		return filingStrategyParam1;
	}

	public void setFilingStrategyParam1(Integer filingStrategyParam1) {
		this.filingStrategyParam1 = filingStrategyParam1;
	}

	public String getFilingStrategyParam2() {
		return filingStrategyParam2;
	}

	public void setFilingStrategyParam2(String filingStrategyParam2) {
		this.filingStrategyParam2 = filingStrategyParam2;
	}

	public Integer getScheduleStrategy() {
		return scheduleStrategy;
	}

	public void setScheduleStrategy(Integer scheduleStrategy) {
		this.scheduleStrategy = scheduleStrategy;
	}

	public Integer getScheduleStrategyParam1() {
		return scheduleStrategyParam1;
	}

	public void setScheduleStrategyParam1(Integer scheduleStrategyParam1) {
		this.scheduleStrategyParam1 = scheduleStrategyParam1;
	}

	public String getScheduleStrategyParam2() {
		return scheduleStrategyParam2;
	}

	public void setScheduleStrategyParam2(String scheduleStrategyParam2) {
		this.scheduleStrategyParam2 = scheduleStrategyParam2;
	}

	public Integer getRealTimeEnabled() {
		return realTimeEnabled;
	}

	public void setRealTimeEnabled(Integer realTimeEnabled) {
		this.realTimeEnabled = realTimeEnabled;
	}

	public Integer getRealTimeInterval() {
		return realTimeInterval;
	}

	public void setRealTimeInterval(Integer realTimeInterval) {
		this.realTimeInterval = realTimeInterval;
	}

	public Integer getEnabled() {
		return enabled;
	}

	public void setEnabled(Integer enabled) {
		this.enabled = enabled;
	}

}
