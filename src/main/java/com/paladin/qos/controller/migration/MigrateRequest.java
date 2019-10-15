package com.paladin.qos.controller.migration;

import java.util.Date;

public class MigrateRequest {

	private String id;
	
	private Date startTime;
	private Date endTime;

	private int migrateNum = 500;

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public int getMigrateNum() {
		return migrateNum;
	}

	public void setMigrateNum(int migrateNum) {
		this.migrateNum = migrateNum;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
