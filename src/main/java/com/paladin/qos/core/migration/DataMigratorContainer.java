package com.paladin.qos.core.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.spring.SpringBeanHelper;
import com.paladin.framework.spring.SpringContainer;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskManager;
import com.paladin.qos.core.mixed.DefaultTaskStack;
import com.paladin.qos.core.mixed.MatrixTaskStack;
import com.paladin.qos.core.mixed.MixedDataTask;
import com.paladin.qos.core.mixed.TaskStack;
import com.paladin.qos.model.migration.DataMigration;
import com.paladin.qos.service.migration.DataMigrationService;

@Component
public class DataMigratorContainer implements SpringContainer {

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	@Autowired
	private DataMigrationService dataMigrationService;

	@Autowired
	private DataTaskManager dataTaskManager;

	private List<IncrementDataMigrator> incrementDataMigratorList;
	private Map<String, IncrementDataMigrator> incrementDataMigratorMap;

	@Override
	public boolean initialize() {

		Map<String, IncrementDataMigrator> migratorSpringMap = SpringBeanHelper.getBeansByType(IncrementDataMigrator.class);

		Map<String, IncrementDataMigrator> migratorIdMap = new HashMap<>();
		if (migratorSpringMap != null) {
			for (IncrementDataMigrator migrator : migratorSpringMap.values()) {
				migratorIdMap.put(migrator.getId(), migrator);
			}
		}

		List<DataMigration> dataMigrations = dataMigrationService.findAll();

		for (DataMigration dataMigration : dataMigrations) {
			int type = dataMigration.getType();
			String id = dataMigration.getId();

			if (type == DataMigration.TYPE_INCREMENT_UPDATE) {
				if (!migratorIdMap.containsKey(id)) {
					IncrementDataMigrator migrator = new CommonIncrementDataMigrator(dataMigration, sqlSessionContainer);
					migratorIdMap.put(migrator.getId(), migrator);
				} else {
					IncrementDataMigrator migrator = migratorIdMap.get(id);
					migrator.setDataMigration(dataMigration);
				}
			} else if (type == DataMigration.TYPE_INCREMENT_UPDATE_YEAR) {
				if (!migratorIdMap.containsKey(id)) {
					IncrementDataMigrator migrator = new YearIncrementDataMigrator(dataMigration, sqlSessionContainer);
					migratorIdMap.put(migrator.getId(), migrator);
				} else {
					IncrementDataMigrator migrator = migratorIdMap.get(id);
					migrator.setDataMigration(dataMigration);
				}
			} else {
				// TODO 以后扩展
			}
		}

		List<IncrementDataMigrator> incrementDataMigratorList = new ArrayList<>();
		incrementDataMigratorList.addAll(migratorIdMap.values());

		this.incrementDataMigratorList = Collections.unmodifiableList(incrementDataMigratorList);
		this.incrementDataMigratorMap = Collections.unmodifiableMap(migratorIdMap);

		registerTask();

		return true;
	}

	private void registerTask() {

		Map<String, List<DataTask>> dataTaskMap = new HashMap<>();

		List<DataTask> realTimeTasks = new ArrayList<>();
		// List<DataTask> dawnTasks = new ArrayList<>();
		List<DataTask> nightTasks = new ArrayList<>();

		for (IncrementDataMigrator migrator : incrementDataMigratorList) {
			DataMigration dataMigration = migrator.getDataMigration();
			DataTask task = new IncrementDataMigrateTask(migrator);

			if (dataMigration.getRealTimeEnabled() == 1) {
				realTimeTasks.add(task);
			} else {
				if (dataMigration.getSeparateProcessThread() == 1) {
					// dawnTasks.add(task);
					nightTasks.add(task);
				} else {
					String originDS = dataMigration.getOriginDataSource();
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
			String id = "migrate-mixed-" + entry.getKey();
			MixedDataTask task = new MixedDataTask(id, new MatrixTaskStack(stacks, i));
			i++;
			// dawnTasks.add(task);
			nightTasks.add(task);
		}

		// dataTaskManager.registerTaskBeforeDawn(dawnTasks);
		dataTaskManager.registerTaskAtNight(nightTasks);
		dataTaskManager.registerTaskRealTime(realTimeTasks);
	}

	public List<IncrementDataMigrator> getIncrementDataMigratorList() {
		return incrementDataMigratorList;
	}

	public IncrementDataMigrator getIncrementDataMigrator(String id) {
		return incrementDataMigratorMap.get(id);
	}

}
