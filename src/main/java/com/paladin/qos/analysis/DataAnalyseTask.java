package com.paladin.qos.analysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.AnalysisService;
import com.paladin.qos.util.TimeUtil;

public class DataAnalyseTask extends DataTask {

	private static Logger logger = LoggerFactory.getLogger(DataAnalyseTask.class);

	private DataAnalyst analyst;
	private AnalysisService analysisService;

	private Map<String, Long> lastanalysedDayMap;
	private int analysedNum;
	private Date processTime;

	public DataAnalyseTask(DataAnalyst analyst, AnalysisService analysisService) {
		super(analyst.getEventId(), DataTask.LEVEL_MAJOR);
		DataEvent event = analyst.getDataEvent();

		this.setConfiguration(event);
		List<Object> labels = new ArrayList<>();
		labels.add(analyst.getEventId());
		labels.add(event.getName());
		labels.add(event.getDataSource());
		this.setLabels(labels);

		this.analyst = analyst;
		this.analysisService = analysisService;
	}

	@Override
	public void doTask() {

//		processTime = new Date();
//		analysedNum = 0;
//
//		if (lastanalysedDayMap == null) {
//			lastanalysedDayMap = getLastanalysedDay();
//		}
//
//		// 归档日期，该日期之后的数据都是很可能会变的，所以标识未未确认
//		Date filingDate = getScheduleFilingDate();
//		long filingTime = filingDate == null ? 0 : filingDate.getTime();
//
//		DataEvent event = analyst.getDataEvent();
//		int maximumProcess = event.getMaximumProcess();
//
//		// 截止日期不能超过今天
//		long endTime = TimeUtil.toDayTime(new Date()).getTime();
//		int eventCount = 0;
//
//		try {
//			for (DataUnit unit : analyst.getTargetUnits()) {
//				String unitId = unit.getId();
//
//				long startTime = lastanalysedDayMap.get(unitId);
//
//				if (filingTime > 0) {
//					if (startTime > filingTime) {
//						startTime = filingTime;
//					}
//				}
//
//				startTime += TimeUtil.MILLIS_IN_DAY;
//				int count = 0;
//
//				while (startTime <= endTime) {
//					Date start = new Date(startTime);
//					startTime += TimeUtil.MILLIS_IN_DAY;
//					Date end = new Date(startTime);
//					boolean confirmed = start.getTime() <= filingTime;
//					boolean success = analysisService.processDataForOneDay(start, end, unitId, analyst, confirmed);
//
//					if (!success) {
//						break;
//					}
//
//					count++;
//					// 更新处理事件
//					lastanalysedDayMap.put(unitId, start.getTime());
//
//					if (maximumProcess > 0 && count >= maximumProcess) {
//						break;
//					}
//
//					if (!isRealTime() && isThreadFinished()) {
//						break;
//					}
//				}
//				eventCount += count;
//			}
//			analysedNum = eventCount;
//			logger.info("数据预处理任务[" + getId() + "]执行完毕，共处理数据：" + eventCount + "条");
//		} catch (Exception e) {
//			logger.error("数据预处理异常[ID:" + getId() + "]", e);
//		}
	}

	/**
	 * 读取最近一次处理日
	 * 
	 * @return
	 */
	private Map<String, Long> getLastanalysedDay() {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String eventId = getId();
		Map<String, Long> lastanalysedDayMap = new HashMap<>();
		for (DataUnit unit : analyst.getTargetUnits()) {
			String unitId = unit.getId();
			Integer dayNum = analysisService.getCurrentDayOfEventAndUnit(eventId, unitId);
			if (dayNum != null) {
				Date date;
				try {
					date = format.parse(String.valueOf(dayNum));
					lastanalysedDayMap.put(unitId, date.getTime());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			} else {
				// 如果一个数据都没，则从默认时间开始
				lastanalysedDayMap.put(unitId, analyst.getDataEvent().getProcessStartDate().getTime() - TimeUtil.MILLIS_IN_DAY);
			}
		}
		return lastanalysedDayMap;
	}

	@Override
	public String getExecuteSituation() {
		if (processTime == null) {
			return "还未执行";
		} else {
			String time = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:mm:ss").format(processTime);
			return "在" + time + "预分析数据" + analysedNum + "条";
		}
	}

}
