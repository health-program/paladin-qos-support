package com.paladin.qos.analysis;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.analysis.DataProcessUnit;

/**
 * 数据修复线程，一般在凌晨执行，用于修复和更新数据
 * 
 * @author TontoZhou
 * @since 2019年9月27日
 */
public class DataRepairThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(DataRepairThread.class);

	private DataProcessManager processManager;
	private DataProcessContainer processContainer;

	// 修复事件
	private List<DataProcessEvent> events;
	// 线程结束时间
	private long threadEndTime;
	// 事件-修复数 map
	private Map<String, Integer> repairCountMap;
	// 最大修复次数
	private int maxRepairCount;

	public DataRepairThread(DataProcessManager processManager, DataProcessContainer processContainer, List<DataProcessEvent> events, long threadEndTime,
			int maxRepairCount) {
		this.processManager = processManager;
		this.processContainer = processContainer;
		this.events = events;
		this.threadEndTime = threadEndTime;
		this.maxRepairCount = maxRepairCount > 0 ? maxRepairCount : 365;
		this.repairCountMap = new HashMap<>();
	}

	@Override
	public void run() {
		try {
			logger.info("--------->开始修复数据任务<---------");
			for (DataProcessEvent event : events) {

				if (!event.isEnabled()) {
					continue;
				}

				String eventId = event.getId();
				int eventCount = 0;

				DataProcessor dataProcessor = processContainer.getDataProcessor(eventId);
				List<DataProcessUnit> units = event.getTargetUnits();

				// 归档日期，该日期之后的数据都是很可能会变的，所以标识未未确认
				long filingTime = TimeUtil.getFilingDate(event).getTime();
				// 截止日期不能超过今天
				long endTime = TimeUtil.toDayTime(new Date()).getTime();

				for (DataProcessUnit unit : units) {
					String unitId = unit.getId();
					long startTime = event.getLastProcessedDay(unitId);
					if (startTime > filingTime) {
						startTime = filingTime;
					}
					startTime += TimeUtil.MILLIS_IN_DAY;
					int count = 0;
					while (startTime <= endTime) {
						Date start = new Date(startTime);
						startTime += TimeUtil.MILLIS_IN_DAY;
						Date end = new Date(startTime);
						boolean confirmed = start.getTime() <= filingTime;
						boolean success = processManager.processDataForOneDay(start, end, unitId, dataProcessor, confirmed);

						if (!success) {
							break;
						}

						if (++count >= maxRepairCount) {
							break;
						}

						if (threadEndTime > 0 && threadEndTime < System.currentTimeMillis()) {
							break;
						}
					}
					
					eventCount += count;
					if (threadEndTime > 0 && threadEndTime < System.currentTimeMillis()) {
						break;
					}
				}
				repairCountMap.put(eventId, eventCount);
				
				if (threadEndTime > 0 && threadEndTime < System.currentTimeMillis()) {
					break;
				}
			}

			// 打印次数
			for (Entry<String, Integer> entry : repairCountMap.entrySet()) {
				logger.info("共处理事件[" + entry.getKey() + "]数据次数：" + entry.getValue() + "次");
			}
		} catch (Exception e) {
			logger.error("修复更新数据异常", e);
		} finally {
			logger.info("--------->修复数据任务结束<---------");
			processManager.readLastProcessedDay(events);
		}
	}
}
