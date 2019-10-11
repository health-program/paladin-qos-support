package com.paladin.qos.migration;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.framework.utils.time.DateFormatUtil;
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
public class DataMigrateRepairThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(DataMigrateRepairThread.class);

	private DataMigratorStack migratorStack;

	// 线程结束时间
	private long threadEndTime;

	public DataMigrateRepairThread(DataMigratorStack migratorStack, long threadEndTime) {
		this.migratorStack = migratorStack;
		this.threadEndTime = threadEndTime;
	}

	@Override
	public void run() {
		try {
			logger.info("--------->开始数据迁移任务<---------");

			while (true) {
				IncrementDataMigrator migrator = migratorStack.pop();

				if (migrator == null) {
					break;
				}

				DataMigration dataMigration = migrator.getDataMigration();

				if (dataMigration.getEnabled() != 1) {
					continue;
				}

				if (!migrator.needScheduleToday()) {
					continue;
				}

				String id = migrator.getId();

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
					migrator.setScheduleStartTime(result.getMigrateEndTime());
					int num = result.getMigrateNum();
					count += num;

					if (threadEndTime > 0 && threadEndTime < System.currentTimeMillis()) {
						break;
					}

					if (!result.isSuccess() || num < selectLimit || (maximumMigrate > 0 && count >= maximumMigrate)) {
						break;
					}

					time = result.getMigrateEndTime();
				} while (true);

				SimpleDateFormat format = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm");
				logger.info("计划迁移数据任务[ID:" + id + "]执行" + (result.isSuccess() ? "完成" : "未完成") + "，迁移数据条数：" + count + "条，迁移开始时间：" + format.format(startTime)
						+ "，迁移结束时间：" + format.format(result.getMigrateEndTime()));
			}
		} catch (Exception e) {
			logger.error("数据迁移异常", e);
		} finally {
			logger.info("--------->数据迁移任务结束<---------");
		}
	}

}
