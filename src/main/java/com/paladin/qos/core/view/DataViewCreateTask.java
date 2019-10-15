package com.paladin.qos.core.view;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.core.DataTask;

public class DataViewCreateTask extends DataTask {

	private static Logger logger = LoggerFactory.getLogger(DataViewCreateTask.class);

	private DataViewCreator creator;

	public DataViewCreateTask(DataViewCreator creator) {
		super(creator.getId());
		this.setConfiguration(creator.getDataView());
		this.creator = creator;
	}

	@Override
	public void doTask() {
		boolean success = creator.updateView();
		logger.info("更新视图[" + creator.getId() + "]" + (success ? "成功" : "失败"));
	}

}
