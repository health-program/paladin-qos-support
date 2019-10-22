package com.paladin.qos.core.migration;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.core.DataMigrateMapper;
import com.paladin.qos.model.migration.DataMigration;
import com.paladin.qos.util.TimeUtil;

public class YearIncrementDataMigrator extends CommonIncrementDataMigrator {

	private static Logger logger = LoggerFactory.getLogger(YearIncrementDataMigrator.class);

	public YearIncrementDataMigrator(DataMigration dataMigration, SqlSessionContainer sqlSessionContainer) {
		super(dataMigration, sqlSessionContainer);
	}

	public MigrateResult migrateData(Date updateStartTime, Date updateEndTime, int migrateNum) {
		MigrateResult result = new MigrateResult(updateStartTime);
		try {
			migrateData(updateStartTime, updateEndTime, result);
		} catch (Exception e) {
			logger.error("迁移数据失败！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime, e);
			result.setSuccess(false);
		}

		return result;
	}

	protected void migrateData(Date updateStartTime, Date updateEndTime, MigrateResult result) {
		if (updateEndTime == null) {
			updateEndTime = new Date();
		}

		List<Map<String, Object>> datas = getData(updateStartTime, updateEndTime);

		if (datas != null) {
			for (Map<String, Object> data : datas) {
				Map<String, Object> needData = processData(data);
				int code = insertOrUpdateData(needData);
				if (code > 0) {
					if (code == 1) {
						result.setInsertedNum(result.getInsertedNum() + 1);
					} else {
						result.setUpdatedNum(result.getUpdatedNum() + 1);
					}
				} else {
					logger.error("更新或插入数据失败！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime);
					result.setSuccess(false);
					return;
				}
			}
		}

		Calendar c = Calendar.getInstance();
		int thisYear = c.get(Calendar.YEAR);
		c.setTime(updateEndTime);
		int updateYear = c.get(Calendar.YEAR);

		if (updateYear < thisYear) {
			c.add(Calendar.YEAR, 1);
			result.setMigrateEndTime(c.getTime());
		} else {
			result.setMigrateEndTime(updateEndTime);
		}
	}

	protected List<Map<String, Object>> getData(Date updateStartTime, Date updateEndTime) {
		sqlSessionContainer.setCurrentDataSource(originDataSource);
		DataMigrateMapper mapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);

		List<String> years = new ArrayList<>();

		int startYear = TimeUtil.getYear(updateStartTime);
		int endYear = TimeUtil.getYear(updateEndTime);

		do {
			years.add(String.valueOf(startYear));
			startYear++;
		} while (startYear < endYear);

		return mapper.selectDataByYear(originTableName, updateTimeField, selectColumns, years);
	}

	@Override
	public Date getCurrentUpdateTime() {
		sqlSessionContainer.setCurrentDataSource(targetDataSource);
		DataMigrateMapper sqlMapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);
		List<String> current = sqlMapper.getMaxUpdateTimeByYear(targetTableName, updateTimeField);
		if (current != null && current.size() > 0) {
			int year = Integer.valueOf(current.get(0));
			Calendar c = Calendar.getInstance();
			c.set(Calendar.YEAR, year);
			return c.getTime();
		} else {
			return defaultStartDate;
		}
	}
}
