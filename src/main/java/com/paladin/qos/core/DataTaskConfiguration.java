package com.paladin.qos.core;

public class DataTaskConfiguration {

	/** 固定默认策略，数据执行到当前时间 */
	public final static int FILING_STRATEGY_UNTIL_NOW = 1;
	/** 固定默认策略，数据执行到当前时间的前几天，scheduleStrategyParam1参数表示为具体多少天 */
	public final static int FILING_STRATEGY_UNTIL_DAY = 2;
	/** 固定默认策略，数据执行到当前时间的前几月，scheduleStrategyParam1参数表示为具体多少月 */
	public final static int FILING_STRATEGY_UNTIL_MONTH = 3;
	/** 固定默认策略，数据执行到当前时间的前几年，scheduleStrategyParam1参数表示为具体多少年 */
	public final static int FILING_STRATEGY_UNTIL_YEAR = 4;
	/** 固定每月某天归档，配合参数1：例如 20，归档日期为当前每月20号 */
	public final static int FILING_STRATEGY_FIXED_DAY_OF_MONTH = 5;
	/** 固定每年某天归档，配合参数1：例如 2/20，归档日期为当前每年2月20号 */
	public final static int FILING_STRATEGY_FIXED_DAY_OF_YEAR = 6;
	/** 自定义归档策略，扩展代码实现 */
	public final static int FILING_STRATEGY_CUSTOM = 10;

	/** 不调度 */
	public final static int SCHEDULE_STRATEGY_NO = 0;
	/** 调度策略：每天执行 */
	public final static int SCHEDULE_STRATEGY_EVERY_DAY = 1;
	/** 调度策略：每个月哪几天执行，配合参数2：例如 1,10,20 表示每月1号、10号、20号会执行 */
	public final static int SCHEDULE_STRATEGY_FIXED_DAY_OF_MONTH = 2;
	/** 调度策略：每年某几天执行，配合参数2：例如 2/1,3/1,4/1 表示2月1号，3月1号，4月1号会执行 */
	public final static int SCHEDULE_STRATEGY_FIXED_DAY_OF_YEAR = 3;
	/** 调度策略：间隔几天执行，配合参数1： 例如 5 则表示每隔5天执行 */
	public final static int SCHEDULE_STRATEGY_INTERVAL_DAY = 5;
	/** 调度策略：间隔几月执行，配合参数1： 例如 5 则表示每隔5月执行 */
	public final static int SCHEDULE_STRATEGY_INTERVAL_MONTH = 6;
	/** 自定义调度策略，需要扩展代码实现 */
	public final static int SCHEDULE_STRATEGY_CUSTOM = 10;

	private String id;
	
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

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
