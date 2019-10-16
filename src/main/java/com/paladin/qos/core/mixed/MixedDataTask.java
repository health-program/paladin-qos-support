package com.paladin.qos.core.mixed;

import com.paladin.qos.core.DataTask;

@Deprecated
public class MixedDataTask extends DataTask {

	private TaskStack stack;

	public MixedDataTask(String id, TaskStack stack) {
		super(id);
		this.stack = stack;
	}

	public void run() {
		TaskStack stackCopy = stack.clone();
		do {
			DataTask task = stackCopy.pop();
			if (task == null) {
				break;
			}

			if (!task.isRun()) {
				task.run();
			}

			if (isThreadFinished()) {
				break;
			}

		} while (true);
	}

	public boolean isRun() {
		return false;
	}

	public boolean needScheduleNow() {
		return true;
	}

	@Override
	public void doTask() {
		// DO NOTHING
	}

	@Override
	public String getExecuteSituation() {
		return null;
	}

}
