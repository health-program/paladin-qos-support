package com.paladin.qos.core.mixed;

import com.paladin.qos.core.DataTask;

public class MixedDataTask extends DataTask {

	private TaskStack stack;

	public MixedDataTask(String id, TaskStack stack) {
		super(id);
		this.stack = stack;
	}

	public void run() {
		do {
			DataTask task = stack.pop();
			if (task == null) {
				break;
			}
			task.run();
		} while (true);
	}

	@Override
	public void doTask() {
		// DO NOTHING
	}

}
