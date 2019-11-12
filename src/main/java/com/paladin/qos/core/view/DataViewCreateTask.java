package com.paladin.qos.core.view;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.model.data.DataView;

public class DataViewCreateTask extends DataTask {

	private static Logger logger = LoggerFactory.getLogger(DataViewCreateTask.class);

	private DataViewCreator creator;
	private Date updateTime;
	private boolean success;

	public DataViewCreateTask(DataViewCreator creator) {
		super(creator.getId(), DataTask.LEVEL_MAJOR);
		DataView view = creator.getDataView();

		this.setConfiguration(view);
		List<Object> labels = new ArrayList<>();
		labels.add(creator.getId());
		labels.add(view.getName());
		labels.add(view.getOriginDataSource());
		this.setLabels(labels);
		this.creator = creator;
	}

	@Override
	public void doTask() {
		success = creator.updateView();
		updateTime = new Date();
		logger.info("更新视图[" + creator.getId() + "]" + (success ? "成功" : "失败"));
	}

	@Override
	public String getExecuteSituation() {
		if (success) {
			String time = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:mm:ss").format(updateTime);
			return "在" + time + "更新视图成功";
		} else {
			if (updateTime != null) {
				String time = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:mm:ss").format(updateTime);
				return "在" + time + "更新视图失败";
			} else {
				return "还未执行";
			}
		}
	}

}
