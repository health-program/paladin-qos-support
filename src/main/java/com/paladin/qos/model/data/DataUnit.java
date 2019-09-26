package com.paladin.qos.model.data;

import javax.persistence.Id;

public class DataUnit {

	/** 医院 */
	public final static int TYPE_HOSPITAL = 1;
	/** 社区 */
	public final static int TYPE_COMMUNITY = 2;
	
	
	
	// 
	@Id
	private String id;

	// 医院名称
	private String name;
	
	// 类型
	private Integer type;
	
	// 妇幼编码
	private String fuyouCode;
	
	// 工位编码
	private String gongweiCode;
	
	// 备注说明
	private String note;
	
	// 排序号
	private Integer orderNum;
	
	// 数据库CODE
	private String dbCode;

	//床位数量
	private Integer bedNumber;

	public Integer getBedNumber() {
		return bedNumber;
	}

	public void setBedNumber(Integer bedNumber) {
		this.bedNumber = bedNumber;
	}

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

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public String getFuyouCode() {
		return fuyouCode;
	}

	public void setFuyouCode(String fuyouCode) {
		this.fuyouCode = fuyouCode;
	}

	public Integer getOrderNum() {
		return orderNum;
	}

	public void setOrderNum(Integer orderNum) {
		this.orderNum = orderNum;
	}

	public String getDbCode() {
		return dbCode;
	}

	public void setDbCode(String dbCode) {
		this.dbCode = dbCode;
	}

	public String getGongweiCode() {
		return gongweiCode;
	}

	public void setGongweiCode(String gongweiCode) {
		this.gongweiCode = gongweiCode;
	}

}