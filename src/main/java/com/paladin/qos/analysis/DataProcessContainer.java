package com.paladin.qos.analysis;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.paladin.framework.spring.SpringBeanHelper;
import com.paladin.framework.spring.SpringContainer;


@Component
public class DataProcessContainer implements SpringContainer {

	private static Logger logger = LoggerFactory.getLogger(DataProcessContainer.class);
	
	private Map<String, DataProcessor> processorMap = new HashMap<>();

	@Override
	public boolean initialize() {

		Map<String, DataProcessor> processorSpringMap = SpringBeanHelper.getBeansByType(DataProcessor.class);
		Map<String, DataProcessor> processorMap = new HashMap<>();

		for (Entry<String, DataProcessor> entry : processorSpringMap.entrySet()) {
			DataProcessor processor = entry.getValue();

			String eventId = processor.getEventId();

			if (processorMap.containsKey(eventId)) {
				logger.warn("===>已经存在数据预处理器[EVENT_ID:" + eventId + "]，该处理器会被忽略");
				continue;
			}

			processorMap.put(eventId, processor);
		}

		this.processorMap = Collections.unmodifiableMap(processorMap);

		return true;
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

}
