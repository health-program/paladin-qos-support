package com.paladin.qos.model.school;

import com.paladin.framework.common.BaseModel;

import javax.persistence.Id;

public class OrgSchoolName extends BaseModel {

    public static final String COLUMN_PARENT_ID= "parentId";
    
	// 
	@Id
	private String id;

	// 学校全称
	private String schoolFullName;
	
	//父ID
	private String parentId;

	//级别
	private String level;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSchoolFullName() {
		return schoolFullName;
	}

	public void setSchoolFullName(String schoolFullName) {
		this.schoolFullName = schoolFullName;
	}

	public String getParentId() {
	    return parentId;
	}

	public void setParentId(String parentId) {
	    this.parentId = parentId;
	}

	public String getLevel() {
	    return level;
	}

	public void setLevel(String level) {
	    this.level = level;
	}

}