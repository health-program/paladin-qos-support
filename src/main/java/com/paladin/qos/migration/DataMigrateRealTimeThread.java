package com.paladin.qos.migration;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.migration.DataMigrateManager.DataMigratorStack;
import com.paladin.qos.migration.increment.IncrementDataMigrator;
import com.paladin.qos.migration.increment.IncrementDataMigrator.MigrateResult;
import com.paladin.qos.model.migration.DataMigration;

/**
 * 数据迁移修复线程
 * 
 * @author TontoZhou
 * @since 2019年10月11日
 */
public class DataMigrateRealTimeThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(DataMigrateRealTimeThread.class);

	private DataMigratorStack migratorStack;

	public DataMigrateRealTimeThread(DataMigratorStack migratorStack) {
		this.migratorStack = migratorStack;
	}

	@Override
	public void run() {

		logger.info("--------->开始数据迁移任务<---------");

		while (true) {
			IncrementDataMigrator migrator = migratorStack.pop();

			if (migrator == null) {
				break;
			}

			try {
				if (!migrator.getLock()) {
					continue;
				}

				DataMigration dataMigration = migrator.getDataMigration();

				// 超过实时更新间隔才会实现实时更新
				long realTime = migrator.getRealTimeMigrateTime();
				if (System.currentTimeMillis() - realTime < dataMigration.getRealTimeInterval() * 60 * 1000) {
					continue;
				}

				Date startTime = migrator.getScheduleStartTime();
				Date filingDate = migrator.getScheduleFilingDate();

				if (filingDate != null && filingDate.getTime() < startTime.getTime()) {
					startTime = filingDate;
				}

				int maximumMigrate = dataMigration.getMaximumMigrate();
				int selectLimit = dataMigration.getSelectDataLimit();
				int count = 0;

				Date time = new Date(startTime.getTime());
				MigrateResult result = null;

				do {
					result = migrator.migrateData(time, null, selectLimit);
					Date updateTime = result.getMigrateEndTime();
					migrator.setScheduleStartTime(updateTime);
					migrator.setRealTimeMigrateTime(updateTime.getTime());
					int num = result.getMigrateNum();
					count += num;

					if (!result.isSuccess() || num < selectLimit || (maximumMigrate > 0 && count >= maximumMigrate)) {
						break;
					}

					time = result.getMigrateEndTime();
				} while (true);

				logger.info("实时更新数据[" + migrator.getId() + "]一次，完成数据：" + count + "条");

			} catch (Exception e) {
				logger.error("数据迁移异常", e);
			} finally {
				migrator.cancelLock();
			}
		}

		logger.info("--------->数据迁移任务结束<---------");
	}

}
