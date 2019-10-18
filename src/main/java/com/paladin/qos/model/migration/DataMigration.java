package com.paladin.qos.model.migration;

import java.util.Date;

import javax.persistence.Id;

import com.paladin.qos.core.DataTaskConfiguration;

public class DataMigration extends DataTaskConfiguration {

	public final static String ORIGIN_DATA_SOURCE_TYPE_MYSQL = "mysql";
	public final static String ORIGIN_DATA_SOURCE_TYPE_SQLSERVER = "sqlserver";
	public final static String ORIGIN_DATA_SOURCE_TYPE_ORACLE = "oracle";

	public final static int TYPE_INCREMENT_UPDATE = 10;
	public final static int TYPE_INCREMENT_UPDATE_YEAR = 11;
	public final static int TYPE_ALL_UPDATE = 20;

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
	
	// 创建时间时间字段
	private String createTimeField;
	
	// 更新时间
	private Integer updateTimeNullable;

	// 主键字段
	private String primaryKeyField;
	
	// 查询列
	private String selectColumns;

	// 每次查询条数限制
	private Integer selectDataLimit;

	// 最大迁移条数
	private Integer maximumMigrate;

	// 缺省开始处理日期
	private Date defaultStartDate;

	// 单独处理线程
	private Integer separateProcessThread;

	// 是否启用毫秒
	private Integer millisecondEnabled;

	// 备注
	private String notes;

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

	public Integer getMillisecondEnabled() {
		return millisecondEnabled;
	}

	public void setMillisecondEnabled(Integer millisecondEnabled) {
		this.millisecondEnabled = millisecondEnabled;
	}

	public String getSelectColumns() {
		return selectColumns;
	}

	public void setSelectColumns(String selectColumns) {
		this.selectColumns = selectColumns;
	}

	public String getCreateTimeField() {
		return createTimeField;
	}

	public void setCreateTimeField(String createTimeField) {
		this.createTimeField = createTimeField;
	}

	public Integer getUpdateTimeNullable() {
		return updateTimeNullable;
	}

	public void setUpdateTimeNullable(Integer updateTimeNullable) {
		this.updateTimeNullable = updateTimeNullable;
	}

}