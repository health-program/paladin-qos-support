package com.paladin.qos.analysis;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import com.paladin.qos.analysis.DataConstantContainer.Event;
import com.paladin.qos.analysis.DataConstantContainer.Unit;

public class DataRealTimeProcessor {

	private static Logger logger = LoggerFactory.getLogger(DataRealTimeProcessor.class);

	@Autowired
	private DataProcessContainer dataProcessContainer;

	private Map<String, DataRealTimeCache> cacheMap = new HashMap<>();
	private Map<String, Integer> updateIntervalTimeMap = new HashMap<>();

	@Scheduled(cron = "0 0/10 * * * ?")
	public void processSchedule(int minutes) {

		List<Event> events = DataConstantContainer.getEventList();
		long begin = System.currentTimeMillis();

		for (Event event : events) {
			String eventId = event.getId();
			DataProcessor processor = dataProcessContainer.getDataProcessor(eventId);

			if (processor != null && event.isRealTimeEnabled()) {
				DataRealTimeCache cache = cacheMap.get(eventId);
				if (cache == null) {
					cache = new DataRealTimeCache();
					cache.setEventId(eventId);

					Date[] dates = processor.getDataRealTimeInterval();
					cache.setStartTime(dates[0]);
					cache.setEndTime(dates[1]);

					cacheMap.put(eventId, cache);
				}

				Integer updateIntervalTime = updateIntervalTimeMap.get(eventId);
				if (updateIntervalTime != null && updateIntervalTime > 0) {
					updateIntervalTimeMap.put(eventId, updateIntervalTime - minutes);
					continue;
				}

				int targetType = event.getTargetType();
				List<Unit> units = DataConstantContainer.getUnitListByType(targetType);

				if (units == null) {
					logger.error("实时处理数据失败！事件[" + eventId + ":" + event.getName() + "]找不到对应的数据范围类型[targetType:" + targetType + "]");
					continue;
				}

				Map<String, DataRealTime> dataUnitMap = processor.getDataRealTime(units);
				cache.setDataUnitMap(dataUnitMap);

				int intervalTime = event.getRealTimeInterval();
				updateIntervalTimeMap.put(eventId, intervalTime);
			}
		}

		logger.info("------------>更新一次数据缓存，耗时：" + (System.currentTimeMillis() - begin) + "毫秒<------------");
	}

	public Map<String, DataRealTime> getRealTimeData(String eventId, List<String> unitIds, Date startTime, Date endTime) {
		Event event = DataConstantContainer.getEvent(eventId);
		if (event != null && event.isRealTimeEnabled() && unitIds != null && unitIds.size() > 0 && startTime != null && endTime != null) {
			DataRealTimeCache cache = cacheMap.get(eventId);
			if (cache != null) {
				Date start = cache.getStartTime();
				Date end = cache.getEndTime();
				if (startTime.getTime() <= start.getTime() && endTime.getTime() >= end.getTime()) {
					Map<String, DataRealTime> dataUnitMap = cache.dataUnitMap;
					if (dataUnitMap != null) {
						return dataUnitMap;
					}
				}
			}

			DataProcessor processor = dataProcessContainer.getDataProcessor(eventId);
			if (processor != null) {
				Map<String, DataRealTime> dataUnitMap = new HashMap<>();
				// 没有缓存则直接查询
				for (String unitId : unitIds) {
					long totalNum = processor.getTotalNum(startTime, endTime, unitId);
					long eventNum = processor.getEventNum(startTime, endTime, unitId);
					DataRealTime data = new DataRealTime(unitId, totalNum, eventNum);
					dataUnitMap.put(unitId, data);
				}

				return dataUnitMap;
			}
		}

		// 数据不完整或不需要实时
		return null;
	}

	public static class DataRealTimeCache {

		private String eventId;

		private Date startTime;
		private Date endTime;

		private Map<String, DataRealTime> dataUnitMap;

		public String getEventId() {
			return eventId;
		}

		public void setEventId(String eventId) {
			this.eventId = eventId;
		}

		public Map<String, DataRealTime> getDataUnitMap() {
			return dataUnitMap;
		}

		public void setDataUnitMap(Map<String, DataRealTime> dataUnitMap) {
			this.dataUnitMap = dataUnitMap;
		}

		public Date getStartTime() {
			return startTime;
		}

		public void setStartTime(Date startTime) {
			this.startTime = startTime;
		}

		public Date getEndTime() {
			return endTime;
		}

		public void setEndTime(Date endTime) {
			this.endTime = endTime;
		}

	}

}
