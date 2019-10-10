package com.paladin.qos.migration.increment.impl.gongwei;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.migration.GongweiDataMigrateMapper;
import com.paladin.qos.migration.increment.CommonIncrementDataMigrator;
import com.paladin.qos.model.migration.DataMigration;
import com.paladin.qos.util.TimeUtil;

public class DataMigrator200001 extends CommonIncrementDataMigrator {

	public DataMigrator200001(DataMigration dataMigration, SqlSessionContainer sqlSessionContainer) {
		super(dataMigration, sqlSessionContainer);
	}

	protected List<Map<String, Object>> getData(Date updateStartTime, Date updateEndTime, int limit) {
		sqlSessionContainer.setCurrentDataSource(originDataSource);
		GongweiDataMigrateMapper mapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(GongweiDataMigrateMapper.class);

		List<String> years = new ArrayList<>();

		int startYear = TimeUtil.getYear(updateStartTime);

		if (updateEndTime != null) {
			int endYear = TimeUtil.getYear(updateStartTime);
			do {
				years.add(String.valueOf(startYear));
				startYear++;
			} while (startYear < endYear);
		} else {
			years.add(String.valueOf(startYear));
		}

		return mapper.selectData200001(years);
	}

}
