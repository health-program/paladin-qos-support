package com.paladin.qos.analysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
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
import com.paladin.qos.analysis.DataProcessUnit;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.model.data.DataProcessException;
import com.paladin.qos.model.data.DataProcessedDay;
import com.paladin.qos.service.analysis.AnalysisService;
import com.paladin.qos.service.data.DataProcessExceptionService;
import com.paladin.qos.service.data.DataProcessedDayService;

@Component
public class DataProcessManager {

	private static Logger logger = LoggerFactory.getLogger(DataProcessManager.class);

	@Autowired
	private DataProcessContainer dataProcessContainer;

	@Autowired
	private DataProcessedDayService dataProcessedDayService;

	@Autowired
	private DataProcessExceptionService dataProcessExceptionService;

	@Autowired
	private AnalysisService analysisService;

	// 线程池
	private ExecutorService executorService = new ThreadPoolExecutor(30, 30, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256), // 使用有界队列，避免OOM
			new ThreadPoolExecutor.DiscardPolicy());;

	@Value("${qos.simple-mode}")
	private boolean simpleMode = false;

	private boolean hasReaded = false;

	/**
	 * 读取最近一次处理的时间
	 */
	public synchronized void readLastProcessedDay(List<DataProcessEvent> events) {

		if (simpleMode) {
			return;
		}

		logger.info("-------------开始读取最近事件处理日期-------------");
		if (events == null) {
			events = DataConstantContainer.getEventList();
		}

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		for (DataProcessEvent event : events) {
			String eventId = event.getId();
			for (DataProcessUnit unit : event.getTargetUnits()) {
				String unitId = unit.getId();
				Integer dayNum = analysisService.getCurrentDayOfEventAndUnit(eventId, unitId);
				if (dayNum != null) {
					Date date;
					try {
						date = format.parse(String.valueOf(dayNum));
						event.updateLastProcessedDay(unitId, date.getTime());
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else {
					// 如果一个数据都没，则从默认时间开始
					event.updateLastProcessedDay(unitId, event.getProcessStartDate().getTime() - TimeUtil.MILLIS_IN_DAY);
				}
			}
		}

		hasReaded = true;

		logger.info("-------------读取最近事件处理日期结束-------------");
	}

	/**
	 * 定时修复数据 TODO 暂时为了多处理数据从晚上开始处理数据到第二天凌晨5点，等数据处理差不多后应该改为凌晨执行
	 */
	@Scheduled(cron = "0 0 19 * * ?")
	public void processUpdate() {
		if (simpleMode) {
			return;
		}

		// 根据定时时间修改，例如晚上10点到明天凌晨5点，则加7个小时时间
		long threadEndTime = System.currentTimeMillis() + 10 * 60 * 60 * 1000;

		executorService.execute(
				new DataRepairThread(this, dataProcessContainer, DataConstantContainer.getEventListByDataSource(DSConstant.DS_FUYOU), threadEndTime, 0));
		executorService.execute(
				new DataRepairThread(this, dataProcessContainer, DataConstantContainer.getEventListByDataSource(DSConstant.DS_GONGWEI), threadEndTime, 0));
		executorService.execute(
				new DataRepairThread(this, dataProcessContainer, DataConstantContainer.getEventListByDataSource(DSConstant.DS_JCYL), threadEndTime, 0));
		executorService.execute(
				new DataRepairThread(this, dataProcessContainer, DataConstantContainer.getEventListByDataSource(DSConstant.DS_YIYUAN), threadEndTime, 0));
	}

	@Scheduled(cron = "0 */1 * * * ?")
	public void processRealTime() {
		if (simpleMode || !hasReaded) {
			return;
		}

		Calendar c = Calendar.getInstance();
		int hour = c.get(Calendar.HOUR_OF_DAY);

		// TODO 实时处理应该避免与更新数据处理同时进行，这里需要与更新数据处理任务同时修改
		if (hour <= 5 || hour >= 19) {
			return;
		}

		List<DataProcessEvent> fuyouEvents = new ArrayList<>();
		List<DataProcessEvent> gongweiEvents = new ArrayList<>();
		List<DataProcessEvent> jcylEvents = new ArrayList<>();
		List<DataProcessEvent> yiyuanEvents = new ArrayList<>();

		for (DataProcessEvent event : DataConstantContainer.getEventList()) {
			if (event.needRealTimeUpdate()) {
				if (event.isSeparateProcessThread()) {
					// 单独线程处理
					executorService.execute(new DataRealTimeThread(this, dataProcessContainer, event));
				} else {
					String source = event.getDataSource();
					if (DSConstant.DS_FUYOU.equals(source)) {
						fuyouEvents.add(event);
					} else if (DSConstant.DS_GONGWEI.equals(source)) {
						gongweiEvents.add(event);
					} else if (DSConstant.DS_JCYL.equals(source)) {
						jcylEvents.add(event);
					} else if (DSConstant.DS_YIYUAN.equals(source)) {
						yiyuanEvents.add(event);
					}
				}
			}
		}

		if (fuyouEvents.size() > 0) {
			executorService.execute(new DataRealTimeThread(this, dataProcessContainer, fuyouEvents));
		}
		if (gongweiEvents.size() > 0) {
			executorService.execute(new DataRealTimeThread(this, dataProcessContainer, gongweiEvents));
		}
		if (jcylEvents.size() > 0) {
			executorService.execute(new DataRealTimeThread(this, dataProcessContainer, jcylEvents));
		}
		if (yiyuanEvents.size() > 0) {
			executorService.execute(new DataRealTimeThread(this, dataProcessContainer, yiyuanEvents));
		}

	}

	// 处理一天的数据
	protected boolean processDataForOneDay(Date start, Date end, String unitId, DataProcessor processor, boolean confirmed) {
		try {
			Metadata rateMetadata = processor.processByDay(start, end, unitId);
			if (rateMetadata != null) {
				saveProcessedDataForDay(rateMetadata, confirmed);
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
	protected void saveProcessedDataForDay(Metadata rateMetadata, boolean confirmed) {

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

		DataProcessUnit unit = DataConstantContainer.getUnit(unitId);

		model.setUnitId(unitId);
		model.setUnitType(unit.getType());

		model.setTotalNum(totalNum);
		model.setEventNum(eventNum);
		model.setConfirmed(confirmed ? 1 : 0);
		model.setUpdateTime(new Date());

		if (!dataProcessedDayService.updateOrSave(model)) {
			throw new RuntimeException("持久化日粒度数据失败！");
		}
	}

	// ---------------------------------------------------------------------
	//
	// 由于处理数据可能时间较长，所以使用线程处理，并轮休查询进度
	//
	// ---------------------------------------------------------------------

	private DataProcessThread processThread;
	private int total;

	public synchronized boolean processDataByThread(Date startTime, Date endTime, List<DataProcessUnit> units, List<DataProcessEvent> events) {
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

			processThread = new DataProcessThread(this, dataProcessContainer, events, units, startTime, endTime);
			processThread.start();
			return true;
		}
	}

	public ProcessStatus getProcessDataStatus() {
		if (processThread == null) {
			return new ProcessStatus(0, 0, ProcessStatus.STATUS_NON);
		} else {
			return new ProcessStatus(total, processThread.getProcessedCount(),
					processThread.isFinished() ? ProcessStatus.STATUS_PROCESSED : ProcessStatus.STATUS_PROCESSING);
		}
	}

	public void shutdownProcessThread() {
		if (processThread != null) {
			processThread.shutdown();
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
