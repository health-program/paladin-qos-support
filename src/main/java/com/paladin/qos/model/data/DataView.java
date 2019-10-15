package com.paladin.qos.model.data;

import javax.persistence.Id;

import com.paladin.qos.core.DataTaskConfiguration;

public class DataView extends DataTaskConfiguration {

	//
	@Id
	private String id;

	// 名称
	private String name;

	// 原始数据源
	private String originDataSource;
	
	// 目标数据源
	private String targetDataSource;

	// 查询sql
	private String selectSql;

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

	public String getOriginDataSource() {
		return originDataSource;
	}

	public void setOriginDataSource(String originDataSource) {
		this.originDataSource = originDataSource;
	}

	public String getSelectSql() {
		return selectSql;
	}

	public void setSelectSql(String selectSql) {
		this.selectSql = selectSql;
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

	public String getTargetDataSource() {
		return targetDataSource;
	}

	public void setTargetDataSource(String targetDataSource) {
		this.targetDataSource = targetDataSource;
	}

}