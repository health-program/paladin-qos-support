package com.paladin.qos.model.migration;

import java.util.Date;
import javax.persistence.Id;

public class DataMigration {

	public final static String ORIGIN_DATA_SOURCE_TYPE_MYSQL = "mysql";
	public final static String ORIGIN_DATA_SOURCE_TYPE_SQLSERVER = "sqlserver";
	public final static String ORIGIN_DATA_SOURCE_TYPE_ORACLE = "oracle";
	
	public final static int TYPE_INCREMENT_UPDATE = 10;
	public final static int TYPE_INCREMENT_UPDATE_YEAR = 11;
	public final static int TYPE_ALL_UPDATE = 20;
	
	
	/**
	 * 固定默认策略，数据执行到当前时间
	 */
	public final static int  FILING_STRATEGY_DEFAULT_NOW = 1;
	/**
	 * 固定默认策略，数据执行到当前时间的前几天，scheduleStrategyParam1参数表示为具体多少天
	 */
	public final static int  FILING_STRATEGY_DEFAULT_DAY = 2;	
	/**
	 * 固定默认策略，数据执行到当前时间的前几月，scheduleStrategyParam1参数表示为具体多少月
	 */
	public final static int  FILING_STRATEGY_DEFAULT_MONTH = 3;
	/**
	 * 固定默认策略，数据执行到当前时间的前几年，scheduleStrategyParam1参数表示为具体多少年
	 */
	public final static int  FILING_STRATEGY_DEFAULT_YEAR = 4;
	
	/**
	 * 自定义策略，需要自行扩展代码实现
	 */
	public final static int  FILING_STRATEGY_CUSTOM = 10;

	//
	@Id
	private String id;

	// 名称
	private String name;

	// 迁移类型：1：增量更新，2：全部更新
	private Integer type;

	// 原始数据源
	private String originDataSource;

	// 原始数据源类型
	private String originDataSourceType;

	// 原始数据源表
	private String originTableName;

	// 目标数据源
	private String targetDataSource;

	// 目标数据源表
	private String targetTableName;

	// 更新时间字段
	private String updateTimeField;

	// 主键字段
	private String primaryKeyField;

	// 每次查询条数限制
	private Integer selectDataLimit;

	// 最大迁移条数
	private Integer maximumMigrate;

	// 缺省开始处理日期
	private Date defaultStartDate;

	// 数据归档策略
	private Integer filingStrategy;

	// 调度任务策略参数1，配合调度任务策略用
	private Integer filingStrategyParam1;

	// 调度任务策略参数2，配合调度任务策略用
	private Integer filingStrategyParam2;

	// 是否需要实时
	private Integer realTimeEnabled;

	// 实时间隔，分钟
	private Integer realTimeInterval;

	// 单独处理线程
	private Integer separateProcessThread;

	// 备注
	private String notes;

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

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public String getOriginDataSource() {
		return originDataSource;
	}

	public void setOriginDataSource(String originDataSource) {
		this.originDataSource = originDataSource;
	}

	public String getOriginDataSourceType() {
		return originDataSourceType;
	}

	public void setOriginDataSourceType(String originDataSourceType) {
		this.originDataSourceType = originDataSourceType;
	}

	public String getOriginTableName() {
		return originTableName;
	}

	public void setOriginTableName(String originTableName) {
		this.originTableName = originTableName;
	}

	public String getTargetDataSource() {
		return targetDataSource;
	}

	public void setTargetDataSource(String targetDataSource) {
		this.targetDataSource = targetDataSource;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public Integer getSelectDataLimit() {
		return selectDataLimit;
	}

	public void setSelectDataLimit(Integer selectDataLimit) {
		this.selectDataLimit = selectDataLimit;
	}

	public Integer getMaximumMigrate() {
		return maximumMigrate;
	}

	public void setMaximumMigrate(Integer maximumMigrate) {
		this.maximumMigrate = maximumMigrate;
	}

	public Date getDefaultStartDate() {
		return defaultStartDate;
	}

	public void setDefaultStartDate(Date defaultStartDate) {
		this.defaultStartDate = defaultStartDate;
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

	public Integer getSeparateProcessThread() {
		return separateProcessThread;
	}

	public void setSeparateProcessThread(Integer separateProcessThread) {
		this.separateProcessThread = separateProcessThread;
	}

	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}

	public Integer getEnabled() {
		return enabled;
	}

	public void setEnabled(Integer enabled) {
		this.enabled = enabled;
	}

	public String getUpdateTimeField() {
		return updateTimeField;
	}

	public void setUpdateTimeField(String updateTimeField) {
		this.updateTimeField = updateTimeField;
	}

	public String getPrimaryKeyField() {
		return primaryKeyField;
	}

	public void setPrimaryKeyField(String primaryKeyField) {
		this.primaryKeyField = primaryKeyField;
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

	public Integer getFilingStrategyParam2() {
		return filingStrategyParam2;
	}

	public void setFilingStrategyParam2(Integer filingStrategyParam2) {
		this.filingStrategyParam2 = filingStrategyParam2;
	}

}