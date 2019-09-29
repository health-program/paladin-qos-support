package com.paladin.qos.analysis;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.analysis.DataProcessUnit;

/**
 * 实时处理数据线程
 * 
 * @author TontoZhou
 * @since 2019年9月27日
 */
public class DataRealTimeThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(DataRealTimeThread.class);

	private DataProcessManager processManager;
	private DataProcessContainer processContainer;

	// 修复事件
	private List<DataProcessEvent> events;

	public DataRealTimeThread(DataProcessManager processManager, DataProcessContainer processContainer, List<DataProcessEvent> events) {
		this.processManager = processManager;
		this.processContainer = processContainer;
		this.events = events;
	}

	public DataRealTimeThread(DataProcessManager processManager, DataProcessContainer processContainer, DataProcessEvent event) {
		this.processManager = processManager;
		this.processContainer = processContainer;
		this.events = new ArrayList<>();
		this.events.add(event);
	}

	@Override
	public void run() {

		logger.debug("----------开始处理实时部分数据---------");

		for (DataProcessEvent event : events) {

			if (event.beginRealTimeUpdate()) {
				try {
					String eventId = event.getId();

					DataProcessor dataProcessor = processContainer.getDataProcessor(eventId);
					List<DataProcessUnit> units = event.getTargetUnits();

					// 归档日期，该日期之后的数据都是很可能会变的，所以标识未未确认
					long filingTime = TimeUtil.getFilingDate(event).getTime();
					// 截止日期不能超过今天
					long endTime = TimeUtil.toDayTime(new Date()).getTime();

					for (DataProcessUnit unit : units) {
						String unitId = unit.getId();

						if (event.getLastProcessedDay(unitId) < filingTime) {
							// 如果数据预处理还未达到归档日期，则不作实时处理
							break;
						}

						long startTime = filingTime + TimeUtil.MILLIS_IN_DAY;

						while (startTime <= endTime) {
							Date start = new Date(startTime);
							startTime += TimeUtil.MILLIS_IN_DAY;
							Date end = new Date(startTime);
							boolean success = processManager.processDataForOneDay(start, end, unitId, dataProcessor, false);

							if (!success) {
								break;
							}
						}
					}

				} finally {
					event.setRealTimeUpdateFinished();
				}
			}

		}
		
		logger.debug("----------处理实时部分数据结束---------");
	}
}
