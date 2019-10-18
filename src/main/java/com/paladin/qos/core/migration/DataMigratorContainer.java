package com.paladin.qos.core.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.spring.SpringBeanHelper;
import com.paladin.framework.spring.SpringContainer;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskManager;
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
			int updateTimeNullable = dataMigration.getUpdateTimeNullable();

			if (type == DataMigration.TYPE_INCREMENT_UPDATE) {

				if (!migratorIdMap.containsKey(id)) {
					IncrementDataMigrator migrator = new CommonIncrementDataMigrator(dataMigration, sqlSessionContainer);
					migratorIdMap.put(migrator.getId(), migrator);
				} else {
					IncrementDataMigrator migrator = migratorIdMap.get(id);
					migrator.setDataMigration(dataMigration);
				}

				if (updateTimeNullable == 1) {
					String id2 = id + "-2";
					if (!migratorIdMap.containsKey(id2)) {
						IncrementDataMigrator migrator = new CommonIncrementDataMigrator(id2, dataMigration, true, sqlSessionContainer);
						migratorIdMap.put(migrator.getId(), migrator);
					} else {
						IncrementDataMigrator migrator = migratorIdMap.get(id);
						migrator.setDataMigration(dataMigration);
					}
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

		List<DataTask> realTimeTasks = new ArrayList<>();
		List<DataTask> nightTasks = new ArrayList<>();

		for (IncrementDataMigrator migrator : incrementDataMigratorList) {
			DataMigration dataMigration = migrator.getDataMigration();
			DataTask task = new IncrementDataMigrateTask(migrator);

			if (dataMigration.getRealTimeEnabled() == 1) {
				realTimeTasks.add(task);
			} else {
				nightTasks.add(task);
			}
		}

		dataTaskManager.registerTaskSchedule(nightTasks);
		dataTaskManager.registerTaskRealTime(realTimeTasks);
	}

	public List<IncrementDataMigrator> getIncrementDataMigratorList() {
		return incrementDataMigratorList;
	}

	public IncrementDataMigrator getIncrementDataMigrator(String id) {
		return incrementDataMigratorMap.get(id);
	}

}
