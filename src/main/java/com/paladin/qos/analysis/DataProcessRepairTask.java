package com.paladin.qos.analysis;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskConfiguration;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.AnalysisService;
import com.paladin.qos.util.TimeUtil;

public class DataProcessRepairTask extends DataTask {

	private static Logger logger = LoggerFactory.getLogger(DataProcessRepairTask.class);

	private DataProcessor processor;
	private AnalysisService analysisService;

	private Map<String, Long> lastProcessedDayMap;
	private long startRepairTime;
	private int repairNum;
	private long repairTime;
	private int repairNumOnce = 30;
	private Date lastRepairDate;

	public DataProcessRepairTask(DataProcessor processor, AnalysisService analysisService) {
		super(processor.getEventId() + "-repair", DataTask.LEVEL_MINOR);

		DataEvent dataEvent = processor.getDataEvent();
		DataTaskConfiguration config = new DataTaskConfiguration();

		config.setEnabled(1);
		config.setRealTimeEnabled(0);
		config.setExecuteHours(5);
		config.setScheduleHour(23);
		config.setScheduleStrategy(DataTaskConfiguration.SCHEDULE_STRATEGY_EVERY_DAY);
		config.setFilingStrategy(dataEvent.getFilingStrategy());
		config.setFilingStrategyParam1(dataEvent.getFilingStrategyParam1());
		config.setFilingStrategyParam2(dataEvent.getFilingStrategyParam2());

		this.setConfiguration(config);
		this.processor = processor;
		this.analysisService = analysisService;
		this.startRepairTime = processor.getDataEvent().getProcessStartDate().getTime();

		this.lastProcessedDayMap = new HashMap<>();
		for (DataUnit unit : processor.getTargetUnits()) {
			String unitId = unit.getId();
			this.lastProcessedDayMap.put(unitId, startRepairTime - TimeUtil.MILLIS_IN_DAY);
		}

		this.repairNum = 0;
		this.repairTime = 0L;

		List<Object> labels = new ArrayList<>();
		labels.add(processor.getEventId());
		labels.add(dataEvent.getName());
		labels.add(dataEvent.getDataSource());
		this.setLabels(labels);
	}

	@Override
	public void doTask() {
		lastRepairDate = new Date();

		// 归档日期，该日期之后的数据都是很可能会变的，所以标识未未确认
		Date filingDate = getScheduleFilingDate();
		long endTime = filingDate == null ? TimeUtil.toDayTime(new Date()).getTime() : filingDate.getTime();

		try {
			int totalCount = 0;
			for (DataUnit unit : processor.getTargetUnits()) {
				String unitId = unit.getId();

				long startTime = lastProcessedDayMap.get(unitId);

				startTime += TimeUtil.MILLIS_IN_DAY;
				int count = 0;

				while (count < repairNumOnce) {

					if (startTime > endTime) {
						startTime = startRepairTime;
					}

					Date start = new Date(startTime);
					startTime += TimeUtil.MILLIS_IN_DAY;
					Date end = new Date(startTime);

					boolean success = analysisService.processDataForOneDay(start, end, unitId, processor, true);

					if (!success) {
						break;
					}

					count++;
					repairNum++;

					// 更新处理事件
					lastProcessedDayMap.put(unitId, start.getTime());

					if (!isRealTime() && isThreadFinished()) {
						break;
					}
				}
				totalCount += count;
			}
			logger.info("数据预处理修复任务[" + getId() + "]执行完毕，共修复处理数据：" + totalCount + "条");
		} catch (Exception e) {
			logger.error("数据预处理修复异常[ID:" + getId() + "]", e);
		}
	}

	@Override
	public String getExecuteSituation() {
		if (lastRepairDate != null) {
			String time = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:mm:ss").format(lastRepairDate);
			return "已修复数据：" + repairNum + "条，共花费时间：" + repairTime + "毫秒，最近一次修复于：" + time;
		} else {
			return "还未开始修复";
		}

	}

}
