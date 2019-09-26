package com.paladin.qos.analysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.paladin.framework.utils.uuid.UUIDUtil;
import com.paladin.qos.analysis.DataConstantContainer.Event;
import com.paladin.qos.analysis.DataConstantContainer.Unit;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataProcessException;
import com.paladin.qos.model.data.DataProcessedDay;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.AnalysisService;
import com.paladin.qos.service.data.DataProcessExceptionService;
import com.paladin.qos.service.data.DataProcessedDayService;

@Component
public class DataProcessManager {

	private static Logger logger = LoggerFactory.getLogger(DataProcessManager.class);

	/** 默认开始处理时间 */
	public static Date DEFAULT_START_TIME;

	static {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			DEFAULT_START_TIME = format.parse("2019-01-01");
		} catch (ParseException e) {
		}
	}

	@Autowired
	private DataProcessContainer dataProcessContainer;

	@Autowired
	private DataProcessedDayService dataProcessedDayService;

	@Autowired
	private DataProcessExceptionService dataProcessExceptionService;

	@Autowired
	private AnalysisService analysisService;

	// 线程池
	private ExecutorService executorService;

	// 最近处理时间map
	private Map<String, Map<String, Long>> lastProcessedDayMap = new HashMap<>();

	@Value("${qos.simple-mode}")
	private boolean simpleMode = false;

	/**
	 * 读取最近一次处理的时间
	 */
	public synchronized void readLastProcessedDay(List<Event> events) {
		if (simpleMode) {
			return;
		}

		logger.info("-------------开始读取最近事件处理日期-------------");
		if (events == null) {
			events = DataConstantContainer.getEventList();
		}

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		for (Event event : events) {
			String eventId = event.getId();
			Map<String, Long> unitProcessedDayMap = lastProcessedDayMap.get(eventId);
			if (unitProcessedDayMap == null) {
				unitProcessedDayMap = new HashMap<>();
				lastProcessedDayMap.put(eventId, unitProcessedDayMap);
			}

			List<Unit> units = DataConstantContainer.getUnitListByType(event.getTargetType());
			for (Unit unit : units) {
				String unitId = unit.getId();
				Integer dayNum = analysisService.getCurrentDayOfEventAndUnit(eventId, unitId);

				if (dayNum != null) {
					Date date;
					try {
						date = format.parse(String.valueOf(dayNum));
						unitProcessedDayMap.put(unitId, date.getTime());
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else {
					// 如果一个数据都没，则从默认时间开始
					unitProcessedDayMap.put(unitId, DEFAULT_START_TIME.getTime() - TimeUtil.MILLIS_IN_DAY);
				}
			}
		}

		logger.info("-------------读取最近事件处理日期结束-------------");
	}

	/**
	 * 每天22点开始修复数据，修复到凌晨5点，等到数据修复差不多时需要调整时间为每天凌晨后，避免处理时间截止到前天而不是昨天
	 */
	@Scheduled(cron = "0 0 18 * * ?")
	public void processSchedule() {
		if (executorService == null) {
			executorService = new ThreadPoolExecutor(5, 5, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256), // 使用有界队列，避免OOM
					new ThreadPoolExecutor.DiscardPolicy());
		}

		// 根据定时时间修改，例如晚上10点到明天凌晨5点，则加7个小时时间
		long threadEndTime = System.currentTimeMillis() + 11 * 60 * 60 * 1000;

		executorService.execute(new RepairThread(DataConstantContainer.getEventListByDataSource(DSConstant.DS_FUYOU), null, threadEndTime, 0, 0, 0));
		executorService.execute(new RepairThread(DataConstantContainer.getEventListByDataSource(DSConstant.DS_GONGWEI), null, threadEndTime, 0, 0, 0));
		executorService.execute(new RepairThread(DataConstantContainer.getEventListByDataSource(DSConstant.DS_JCYL), null, threadEndTime, 0, 0, 0));
		executorService.execute(new RepairThread(DataConstantContainer.getEventListByDataSource(DSConstant.DS_YIYUAN), null, threadEndTime, 0, 0, 0));
	}

	private class RepairThread implements Runnable {

		// 修复事件
		private List<Event> events;
		// 线程结束时间
		private long threadEndTime;
		// 修复到最大时间点
		private long maxRepairTime;
		// 修复最新时间点
		private long minRepairTime;
		// 事件-修复数 map
		private Map<String, Integer> repairCountMap;
		// 最大修复次数
		private int maxRepairCount;

		private List<Unit> units = null;
		private boolean judgeUnit = false;

		private boolean finished;

		private RepairThread(List<Event> events, List<Unit> units, long threadEndTime, long minRepairTime, long maxRepairTime, int maxRepairCount) {
			this.events = events;

			if (units != null && units.size() > 0) {
				this.judgeUnit = true;
				this.units = units;
			}

			this.threadEndTime = threadEndTime;
			this.maxRepairTime = maxRepairTime;
			this.minRepairTime = minRepairTime;
			this.maxRepairCount = maxRepairCount > 0 ? maxRepairCount : 365;

			this.repairCountMap = new HashMap<>();
		}

		@Override
		public void run() {
			try {
				logger.info("--------->开始修复数据任务<---------");
				for (Event event : events) {
					String eventId = event.getId();
					int targetType = event.getTargetType();
					int eventCount = 0;

					DataProcessor dataProcessor = dataProcessContainer.getDataProcessor(eventId);

					List<Unit> units = this.units == null ? DataConstantContainer.getUnitListByType(targetType) : this.units;

					Date lastDate = TimeUtil.getTodayBefore(event.getProcessBefore());
					long endTime = maxRepairTime > 0 ? maxRepairTime : lastDate.getTime();

					for (Unit unit : units) {
						int unitType = unit.getType();

						if (judgeUnit) {
							if ((targetType == DataEvent.TARGET_TYPE_HOSPITAL && unitType != DataUnit.TYPE_HOSPITAL)
									|| (targetType == DataEvent.TARGET_TYPE_COMMUNITY && unitType != DataUnit.TYPE_COMMUNITY)) {
								continue;
							}
						}

						String unitId = unit.getId();
						long startTime = minRepairTime > 0 ? minRepairTime : lastProcessedDayMap.get(eventId).get(unitId) + TimeUtil.MILLIS_IN_DAY;

						int count = 0;
						while (startTime <= endTime) {

							Date start = new Date(startTime);
							startTime += TimeUtil.MILLIS_IN_DAY;
							Date end = new Date(startTime);
							boolean success = processDataForOneDay(start, end, unitId, dataProcessor);

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
			} finally {
				finished = true;
				logger.info("--------->修复数据任务结束<---------");
				readLastProcessedDay(events);
			}
		}

		public boolean isFinished() {
			return finished;
		}

		public int getRepairCount() {
			int count = 0;
			for (Entry<String, Integer> entry : repairCountMap.entrySet()) {
				count += entry.getValue();
			}
			return count;
		}

		public void shutdown() {
			this.threadEndTime = 1;
		}

	}

	// 处理一天的数据
	private boolean processDataForOneDay(Date start, Date end, String unitId, DataProcessor processor) {
		try {
			RateMetadata rateMetadata = processor.processByDay(start, end, unitId);
			if (rateMetadata != null) {
				saveProcessedDataForDay(rateMetadata);
				return true;
			}
		} catch (Exception ex) {
			logger.error("处理数据失败！日期：" + start + "，事件：" + processor.getEventId() + "，医院：" + unitId, ex);
			DataProcessException exception = new DataProcessException();
			exception.setId(UUIDUtil.createUUID());
			exception.setCreateTime(new Date());
			exception.setEventId(processor.getEventId());
			exception.setException(ex.getMessage());
			exception.setProcessDay(start);
			exception.setUnitId(unitId);
			try {
				dataProcessExceptionService.save(exception);
			} catch (Exception ex2) {
				logger.error("持久化处理数据异常错误", ex2);
			}
		}
		return false;
	}

	// 保存按天处理的数据
	private void saveProcessedDataForDay(RateMetadata rateMetadata) {

		// 根据日期与事件创建唯一ID
		int year = rateMetadata.getYear();
		int month = rateMetadata.getMonth();
		int day = rateMetadata.getDay();
		String eventId = rateMetadata.getEventId();
		String unitId = rateMetadata.getUnitValue();

		StringBuilder sb = new StringBuilder(eventId);
		sb.append('_').append(unitId).append('_');
		sb.append(year);
		if (month < 10) {
			sb.append('0');
		}
		sb.append(month);
		if (day < 10) {
			sb.append('0');
		}
		sb.append(day);

		String id = sb.toString();

		DataProcessedDay model = new DataProcessedDay();
		model.setId(id);
		model.setEventId(eventId);
		model.setDay(day);
		model.setMonth(month);
		model.setYear(year);
		model.setWeekMonth(rateMetadata.getWeekMonth());
		model.setWeekYear(rateMetadata.getWeekYear());

		int serialNumber = year * 10000 + month * 100 + day;
		model.setSerialNumber(serialNumber);

		long totalNum = rateMetadata.getTotalNum();
		long eventNum = rateMetadata.getEventNum();

		Unit unit = DataConstantContainer.getUnit(unitId);

		model.setUnitId(unitId);
		model.setUnitType(unit.getType());

		model.setTotalNum(totalNum);
		model.setEventNum(eventNum);
		model.setRate(getRate(totalNum, eventNum));
		model.setUpdateTime(new Date());

		if (!dataProcessedDayService.updateOrSave(model)) {
			throw new RuntimeException("持久化日粒度数据失败！");
		}
	}

	// 获取概率
	private int getRate(long totalNum, long eventNum) {
		if (totalNum == 0 || eventNum == 0) {
			return 0;
		}
		long r = eventNum * 100000 / totalNum;
		if (r < 5) {
			return 0;
		}
		r += 5;
		return (int) (r / 10);
	}

	// ---------------------------------------------------------------------
	//
	// 由于处理数据可能时间较长，所以使用线程处理，并轮休查询进度
	//
	// ---------------------------------------------------------------------

	private Thread processThread;
	private RepairThread repairThread;
	private int total;

	public synchronized boolean processDataByThread(Date startTime, Date endTime, List<Unit> units, List<Event> events) {
		if (processThread != null && processThread.isAlive()) {
			return false;
		} else {
			// 计算total
			int days = (int) TimeUtil.getIntervalDays(startTime.getTime(), endTime.getTime());
			if (units != null) {
				total = days * units.size() * events.size();
			} else {
				total = days * DataConstantContainer.getUnitList().size() * events.size();
			}

			repairThread = new RepairThread(events, units, 0, startTime.getTime(), endTime.getTime(), 0);
			processThread = new Thread(repairThread);
			processThread.start();
			return true;
		}
	}

	public ProcessStatus getProcessDataStatus() {
		if (processThread == null || repairThread == null) {
			return new ProcessStatus(0, 0, ProcessStatus.STATUS_NON);
		} else {
			return new ProcessStatus(total, repairThread.getRepairCount(),
					repairThread.isFinished() ? ProcessStatus.STATUS_PROCESSED : ProcessStatus.STATUS_PROCESSING);
		}
	}

	public void shutdownProcessThread() {
		if (repairThread != null) {
			repairThread.shutdown();
		}
	}

	public static class ProcessStatus {

		public final static int STATUS_NON = -1;
		public final static int STATUS_PROCESSING = 1;
		public final static int STATUS_PROCESSED = 2;

		private int total;
		private int current;
		private int status;

		public ProcessStatus(int total, int current, int status) {
			this.total = total;
			this.current = current;
			this.status = status;
		}

		public int getTotal() {
			return total;
		}

		public int getCurrent() {
			return current;
		}

		public int getStatus() {
			return status;
		}

	}
}
