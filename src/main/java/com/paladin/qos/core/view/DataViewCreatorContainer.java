package com.paladin.qos.core.view;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.spring.SpringContainer;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskManager;
import com.paladin.qos.model.data.DataView;
import com.paladin.qos.service.data.DataViewService;

@Component
public class DataViewCreatorContainer implements SpringContainer {

	@Autowired
	private SqlSessionContainer sqlSessionContainer;
	@Autowired
	private DataViewService dataViewService;
	@Autowired
	private DataTaskManager dataTaskManager;

	private Map<String, DataViewCreator> creatorMap;
	private List<DataViewCreator> creatorList;

	@Override
	public boolean initialize() {

		Map<String, DataViewCreator> creatorMap = new HashMap<>();
		List<DataViewCreator> creatorList = new ArrayList<>();

		List<DataView> dataViews = dataViewService.findAll();

		for (DataView dataView : dataViews) {
			DataViewCreator creator = new DataViewCreator(dataView, sqlSessionContainer);
			creatorMap.put(creator.getId(), creator);
			creatorList.add(creator);
		}

		this.creatorList = Collections.unmodifiableList(creatorList);
		this.creatorMap = Collections.unmodifiableMap(creatorMap);

		registerTask();

		return true;
	}

	private void registerTask() {

		List<DataTask> realTimeTasks = new ArrayList<>();
		List<DataTask> nightTasks = new ArrayList<>();

		for (DataViewCreator creator : creatorList) {
			DataViewCreateTask task = new DataViewCreateTask(creator);
			DataView dataView = creator.getDataView();

			if (dataView.getRealTimeEnabled() == 1) {
				realTimeTasks.add(task);
			} else {
				nightTasks.add(task);
			}
		}

		dataTaskManager.registerTaskAtNight(nightTasks);
		dataTaskManager.registerTaskRealTime(realTimeTasks);

	}

	public DataViewCreator getDataViewCreator(String id) {
		return creatorMap.get(id);
	}

	public List<DataViewCreator> getDataViewCreators() {
		return creatorList;
	}

}
