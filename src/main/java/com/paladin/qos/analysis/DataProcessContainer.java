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
import com.paladin.qos.core.mixed.DefaultTaskStack;
import com.paladin.qos.core.mixed.MatrixTaskStack;
import com.paladin.qos.core.mixed.MixedDataTask;
import com.paladin.qos.core.mixed.TaskStack;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.AnalysisService;

@Component
public class DataProcessContainer implements SpringContainer {

	private static Logger logger = LoggerFactory.getLogger(DataProcessContainer.class);

	private Map<String, DataProcessor> processorMap = new HashMap<>();

	@Autowired
	private AnalysisService analysisService;

	@Autowired
	private DataTaskManager dataTaskManager;

	@Override
	public boolean initialize() {

		Map<String, DataProcessor> processorSpringMap = SpringBeanHelper.getBeansByType(DataProcessor.class);
		Map<String, DataProcessor> processorMap = new HashMap<>();

		for (Entry<String, DataProcessor> entry : processorSpringMap.entrySet()) {

			DataProcessor processor = entry.getValue();

			String eventId = processor.getEventId();

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

			processor.setTargetUnits(units);
			processor.setDataEvent(dataEvent);

			if (processorMap.containsKey(eventId)) {
				logger.warn("===>已经存在数据预处理器[EVENT_ID:" + eventId + "]，该处理器会被忽略");
				continue;
			}

			processorMap.put(eventId, processor);
		}

		this.processorMap = Collections.unmodifiableMap(processorMap);

		registerTask();

		return true;
	}

	private void registerTask() {

		Map<String, List<DataTask>> dataTaskMap = new HashMap<>();

		List<DataTask> realTimeTasks = new ArrayList<>();
		// List<DataTask> dawnTasks = new ArrayList<>();
		List<DataTask> nightTasks = new ArrayList<>();

		for (DataProcessor processor : processorMap.values()) {

			DataEvent dataEvent = processor.getDataEvent();
			DataProcessTask task = new DataProcessTask(processor, analysisService);

			if (dataEvent.getRealTimeEnabled() == 1) {
				realTimeTasks.add(task);
			} else {
				if (dataEvent.getSeparateProcessThread() == 1) {
					// dawnTasks.add(task);
					nightTasks.add(task);
				} else {
					String originDS = dataEvent.getDataSource();
					List<DataTask> taskList = dataTaskMap.get(originDS);
					if (taskList == null) {
						taskList = new ArrayList<>();
						dataTaskMap.put(originDS, taskList);
					}
					taskList.add(task);
				}
			}
		}

		List<TaskStack> stacks = new ArrayList<>();
		for (Entry<String, List<DataTask>> entry : dataTaskMap.entrySet()) {
			TaskStack stack = new DefaultTaskStack(entry.getValue());
			stacks.add(stack);
		}

		int i = 0;
		for (Entry<String, List<DataTask>> entry : dataTaskMap.entrySet()) {
			String id = "event-mixed-" + entry.getKey();
			MixedDataTask task = new MixedDataTask(id, new MatrixTaskStack(stacks, i));
			i++;
			// dawnTasks.add(task);
			nightTasks.add(task);
		}

		// dataTaskManager.registerTaskBeforeDawn(dawnTasks);
		dataTaskManager.registerTaskAtNight(nightTasks);
		dataTaskManager.registerTaskRealTime(realTimeTasks);
	}

	/**
	 * 获取数据处理器
	 * 
	 * @param eventId
	 * @return
	 */
	public DataProcessor getDataProcessor(String eventId) {
		return processorMap.get(eventId);
	}

	/**
	 * 获取数据处理器集合
	 * 
	 * @return
	 */
	public Collection<DataProcessor> getDataProcessors() {
		return processorMap.values();
	}

	public int order() {
		return 1;
	}

}
