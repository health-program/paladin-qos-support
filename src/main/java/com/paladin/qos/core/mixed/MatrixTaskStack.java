package com.paladin.qos.core.mixed;

import java.util.List;

import com.paladin.qos.core.DataTask;

public class MatrixTaskStack implements TaskStack {

	private List<TaskStack> stacks;
	private int startIndex;
	private int visitCount;

	public MatrixTaskStack(List<TaskStack> stacks, int startIndex) {
		this.stacks = stacks;
		this.startIndex = startIndex;
		this.visitCount = 0;
	}

	public synchronized DataTask pop() {
		while (visitCount < stacks.size()) {
			DataTask task = stacks.get(startIndex).pop();
			if (task == null) {
				startIndex++;
				visitCount++;
				if (startIndex == stacks.size()) {
					startIndex = 0;
				}
			} else {
				return task;
			}
		}
		return null;
	}

}
