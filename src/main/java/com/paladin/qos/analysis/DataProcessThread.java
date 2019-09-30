package com.paladin.qos.analysis;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.analysis.DataProcessUnit;

/**
 * 数据处理线程，处理时间段内，某些事件，某些单位的数据
 * 
 * @author TontoZhou
 * @since 2019年9月27日
 */
public class DataProcessThread extends Thread {

	private static Logger logger = LoggerFactory.getLogger(DataRepairThread.class);

	private DataProcessManager processManager;
	private DataProcessContainer processContainer;

	private List<DataProcessEvent> events;
	private List<DataProcessUnit> units;
	private Date startTime;
	private Date endTime;
	private boolean finished;
	private boolean shutdown = false;
	private int count = 0;

	public DataProcessThread(DataProcessManager processManager, DataProcessContainer processContainer, List<DataProcessEvent> events, List<DataProcessUnit> units,
			Date startTime, Date endTime) {
		this.events = events;
		this.units = units;
		this.startTime = TimeUtil.toDayTime(startTime);
		this.endTime = TimeUtil.toDayTime(endTime);
	}

	@Override
	public void run() {
		try {
			logger.info("--------->开始处理数据任务<---------");

			for (DataProcessEvent event : events) {
				if (!event.isEnabled()) {
					continue;
				}

				String eventId = event.getId();
				DataProcessor dataProcessor = processContainer.getDataProcessor(eventId);
				if (dataProcessor == null) {
					logger.error("统计事件[eventId:" + eventId + "]没有对应处理器");
					continue;
				}

				List<DataProcessUnit> units = this.units == null ? event.getTargetUnits() : this.units;

				// 归档日期，该日期之后的数据都是很可能会变的，所以标识未未确认
				long filingTime = TimeUtil.getFilingDate(event).getTime();
				// 不能超过今天
				long end = endTime.getTime() > System.currentTimeMillis() ? TimeUtil.toDayTime(new Date()).getTime() : endTime.getTime();

				for (DataProcessUnit unit : units) {
					String unitId = unit.getId();

					if (!event.isTargetUnit(unitId)) {
						continue;
					}

					long start = startTime.getTime();

					int count = 0;
					while (start <= end) {
						Date startD = new Date(start);
						start += TimeUtil.MILLIS_IN_DAY;
						Date endD = new Date(start);

						boolean confirmed = startD.getTime() <= filingTime;
						boolean success = processManager.processDataForOneDay(startD, endD, unitId, dataProcessor, confirmed);

						if (!success) {
							break;
						}

						if (shutdown) {
							break;
						}
					}

					this.count += count;

					if (shutdown) {
						break;
					}
				}

				if (shutdown) {
					break;
				}
			}
		} finally {
			finished = true;
			logger.info("--------->处理数据任务结束<---------");
			processManager.readLastProcessedDay(events);
		}
	}

	public int getProcessedCount() {
		return count;
	}

	public boolean isFinished() {
		return finished;
	}

	public void shutdown() {
		this.shutdown = true;
	}

}