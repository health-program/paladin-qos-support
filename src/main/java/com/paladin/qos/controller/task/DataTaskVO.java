package com.paladin.qos.controller.task;

import com.paladin.qos.core.DataTask;

public class DataTaskVO {

	private String id;
	private boolean isRun;

	public DataTaskVO(DataTask task) {
		this.id = task.getId();
		this.isRun = task.isRun();
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

}
