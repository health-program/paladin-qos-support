package com.paladin.qos.core.mixed;

import java.util.List;

import com.paladin.qos.core.DataTask;

public class DefaultTaskStack implements TaskStack {

	private int index;
	private List<DataTask> tasks;

	public DefaultTaskStack(List<DataTask> tasks) {
		this.tasks = tasks;
		this.index = 0;
	}

	public synchronized DataTask pop() {
		if (index >= tasks.size()) {
			return null;
		}

		DataTask task = tasks.get(index);
		index++;
		return task;
	}
}
