package com.paladin.qos.migration;

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
import com.paladin.qos.migration.increment.CommonIncrementDataMigrator;
import com.paladin.qos.migration.increment.IncrementDataMigrator;
import com.paladin.qos.model.migration.DataMigration;
import com.paladin.qos.service.migration.DataMigrationService;

@Component
public class DataMigratorContainer implements SpringContainer {

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	@Autowired
	private DataMigrationService dataMigrationService;

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
			} else {
				// TODO 以后扩展
			}
		}

		List<IncrementDataMigrator> incrementDataMigratorList = new ArrayList<>();
		incrementDataMigratorList.addAll(migratorIdMap.values());

		this.incrementDataMigratorList = Collections.unmodifiableList(incrementDataMigratorList);
		this.incrementDataMigratorMap = Collections.unmodifiableMap(migratorIdMap);

		return true;
	}

	public List<IncrementDataMigrator> getIncrementDataMigratorList() {
		return incrementDataMigratorList;
	}

	public IncrementDataMigrator getIncrementDataMigrator(String id) {
		return incrementDataMigratorMap.get(id);
	}

}
