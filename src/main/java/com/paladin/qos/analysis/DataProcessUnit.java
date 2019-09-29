package com.paladin.qos.analysis;

import com.paladin.qos.model.data.DataUnit;

public class DataProcessUnit {
	
	private DataUnit source;

	private String id;
	private String name;
	private int type;
	private int orderNum;
	

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

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public DataUnit getSource() {
		return source;
	}

	public void setSource(DataUnit source) {
		this.source = source;
	}

	public int getOrderNum() {
		return orderNum;
	}

	public void setOrderNum(int orderNum) {
		this.orderNum = orderNum;
	}
	
}
