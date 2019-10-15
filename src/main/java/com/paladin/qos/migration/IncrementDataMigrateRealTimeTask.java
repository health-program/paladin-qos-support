package com.paladin.qos.migration;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.core.DataTask;
import com.paladin.qos.migration.increment.IncrementDataMigrator;
import com.paladin.qos.migration.increment.IncrementDataMigrator.MigrateResult;
import com.paladin.qos.model.migration.DataMigration;

public class IncrementDataMigrateRealTimeTask extends DataTask {

	private static Logger logger = LoggerFactory.getLogger(IncrementDataMigrateRealTimeTask.class);

	private IncrementDataMigrator dataMigrator;

	public IncrementDataMigrateRealTimeTask(IncrementDataMigrator dataMigrator) {
		super(dataMigrator.getId());
		setConfiguration(dataMigrator.getDataMigration());
		this.dataMigrator = dataMigrator;
	}

	@Override
	public void doTask() {
		try {
			DataMigration dataMigration = dataMigrator.getDataMigration();

			if (!doRealTime()) {
				return;
			}

			if (updateTime == null) {
				updateTime = dataMigrator.getCurrentUpdateTime();
			}

			Date filingDate = getScheduleFilingDate();

			if (filingDate != null) {
				if (updateTime != null && filingDate.getTime() < updateTime.getTime()) {
					updateTime = filingDate;
				}
			}

			int maximumMigrate = dataMigration.getMaximumMigrate();
			int selectLimit = dataMigration.getSelectDataLimit();
			int count = 0;

			MigrateResult result = null;

			do {
				result = dataMigrator.migrateData(updateTime, null, selectLimit);

				updateTime = result.getMigrateEndTime();
				realTimeMillisecond = updateTime.getTime();

				int num = result.getMigrateNum();
				count += num;

				if (!result.isSuccess() || num < selectLimit || (maximumMigrate > 0 && count >= maximumMigrate)) {
					break;
				}

				if (isThreadFinished()) {
					break;
				}

			} while (true);

			logger.info("实时更新数据[" + getId() + "]一次，完成数据：" + count + "条");
		} catch (Exception e) {
			logger.error("实时数据迁移[ " + getId() + "]异常", e);
		}
	}

}
