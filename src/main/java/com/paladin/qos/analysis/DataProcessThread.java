package com.paladin.qos.analysis;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.analysis.DataConstantContainer.Event;
import com.paladin.qos.analysis.DataConstantContainer.Unit;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;

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

	private List<Event> events;
	private List<Unit> units;
	private Date startTime;
	private Date endTime;
	private boolean finished;
	private boolean shutdown = false;
	private int count = 0;

	public DataProcessThread(DataProcessManager processManager, DataProcessContainer processContainer, List<Event> events, List<Unit> units, Date startTime,
			Date endTime) {
		this.events = events;
		this.units = units;
		this.startTime = TimeUtil.toDayTime(startTime);
		this.endTime = TimeUtil.toDayTime(endTime);
	}

	@Override
	public void run() {
		try {
			logger.info("--------->开始处理数据任务<---------");

			for (Event event : events) {
				String eventId = event.getId();
				int targetType = event.getTargetType();

				DataProcessor dataProcessor = processContainer.getDataProcessor(eventId);

				List<Unit> units = this.units == null ? DataConstantContainer.getUnitListByType(targetType) : this.units;

				// 归档日期，该日期之后的数据都是很可能会变的，所以标识未未确认
				long filingTime = TimeUtil.getFilingDate(event).getTime();
				// 不能超过今天
				long end = endTime.getTime() > System.currentTimeMillis() ? TimeUtil.toDayTime(new Date()).getTime() : endTime.getTime();

				for (Unit unit : units) {
					int unitType = unit.getType();

					if ((targetType == DataEvent.TARGET_TYPE_HOSPITAL && unitType != DataUnit.TYPE_HOSPITAL)
							|| (targetType == DataEvent.TARGET_TYPE_COMMUNITY && unitType != DataUnit.TYPE_COMMUNITY)) {
						continue;
					}

					String unitId = unit.getId();
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