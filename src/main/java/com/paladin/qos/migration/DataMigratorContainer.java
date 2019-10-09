package com.paladin.qos.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.spring.SpringContainer;

@Component
public class DataMigratorContainer implements SpringContainer {

	private List<DataMigrator> dataMigratorList = new ArrayList<>();

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	@Override
	public boolean initialize() {

		List<DataMigrator> dataMigratorList = new ArrayList<>();

		IncrementDataMigrator demo = new IncrementDataMigrator();
		demo.setOriginDataSource("fuyou");
		demo.setOriginDataSourceType(IncrementDataMigrator.ORIGIN_DATA_SOURCE_TYPE_SQLSERVER);
		demo.setOriginTableName("Csqyslqffdjb");
		demo.setTargetDataSource("yiyuan");
		demo.setTargetTableName("Csqyslqffdjb");
		demo.setUpdateTimeField("dLasteDate");
		demo.setSqlSessionContainer(sqlSessionContainer);

		Set<String> primaryKeyFields = new HashSet<>();
		primaryKeyFields.add("AutoId");

		demo.setPrimaryKeyFields(primaryKeyFields);

		dataMigratorList.add(demo);

		this.dataMigratorList = Collections.unmodifiableList(dataMigratorList);
		return true;
	}

	public List<DataMigrator> getDataMigratorList() {
		return dataMigratorList;
	}

}
