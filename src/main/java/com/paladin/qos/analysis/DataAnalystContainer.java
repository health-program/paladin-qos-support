package com.paladin.qos.analysis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.framework.spring.SpringBeanHelper;
import com.paladin.framework.spring.SpringContainer;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskManager;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.AnalysisService;

//@Component
public class DataAnalystContainer implements SpringContainer {

	private static Logger logger = LoggerFactory.getLogger(DataAnalystContainer.class);

	private Map<String, DataAnalyst> analystMap = new HashMap<>();

	@Autowired
	private AnalysisService analysisService;

	@Autowired
	private DataTaskManager dataTaskManager;

	@Override
	public boolean initialize() {

		Map<String, DataAnalyst> analystSpringMap = SpringBeanHelper.getBeansByType(DataAnalyst.class);
		Map<String, DataAnalyst> analystMap = new HashMap<>();

		for (Entry<String, DataAnalyst> entry : analystSpringMap.entrySet()) {

			DataAnalyst analyst = entry.getValue();

			String eventId = analyst.getEventId();

			DataEvent dataEvent = DataConstantContainer.getEvent(eventId);

			if (dataEvent == null) {
				logger.error("找不到处理器对应事件：" + eventId);
				continue;
			}

			List<DataUnit> units = null;
			int targetType = dataEvent.getTargetType();

			if (targetType == DataEvent.TARGET_TYPE_ALL) {
				units = DataConstantContainer.getUnitList();
			} else if (targetType == DataEvent.TARGET_TYPE_HOSPITAL) {
				units = DataConstantContainer.getHospitalList();
			} else if (targetType == DataEvent.TARGET_TYPE_COMMUNITY) {
				units = DataConstantContainer.getCommunityList();
			}

			analyst.setTargetUnits(units);
			analyst.setDataEvent(dataEvent);

			if (analystMap.containsKey(eventId)) {
				logger.warn("===>已经存在数据预处理器[EVENT_ID:" + eventId + "]，该处理器会被忽略");
				continue;
			}

			analystMap.put(eventId, analyst);
		}

		this.analystMap = Collections.unmodifiableMap(analystMap);

		registerTask();

		return true;
	}

	private void registerTask() {

//		List<DataTask> realTimeTasks = new ArrayList<>();
//		List<DataTask> nightTasks = new ArrayList<>();
//
//		for (DataAnalyst processor : analystMap.values()) {
//
//			DataEvent dataEvent = processor.getDataEvent();
//			DataProcessTask task = new DataAnalyseTask(processor, analysisService);
//
//			if (dataEvent.getRealTimeEnabled() == 1) {
//				realTimeTasks.add(task);
//			} else {
//				nightTasks.add(task);
//			}
//
//			// 注册修复任务
//			DataProcessRepairTask repairTask = new DataProcessRepairTask(processor, analysisService);
//			nightTasks.add(repairTask);
//		}
//
//		dataTaskManager.registerTaskSchedule(nightTasks);
//		dataTaskManager.registerTaskRealTime(realTimeTasks);
	}

	/**
	 * 获取数据处理器
	 * 
	 * @param eventId
	 * @return
	 */
//	public DataProcessor getDataProcessor(String eventId) {
//		return processorMap.get(eventId);
//	}
//
//	/**
//	 * 获取数据处理器集合
//	 * 
//	 * @return
//	 */
//	public Collection<DataProcessor> getDataProcessors() {
//		return processorMap.values();
//	}

	public int order() {
		return 1;
	}

}
