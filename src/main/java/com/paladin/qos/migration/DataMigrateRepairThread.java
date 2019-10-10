package com.paladin.qos.migration;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.migration.increment.IncrementDataMigrator;
import com.paladin.qos.migration.increment.IncrementDataMigrator.MigrateResult;
import com.paladin.qos.model.migration.DataMigration;

/**
 * 数据修复线程，一般在凌晨执行，用于修复和更新数据
 * 
 * @author TontoZhou
 * @since 2019年9月27日
 */
public class DataMigrateRepairThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(DataMigrateRepairThread.class);

	private List<IncrementDataMigrator> migrators;

	// 线程结束时间
	private long threadEndTime;

	public DataMigrateRepairThread(List<IncrementDataMigrator> migrators, long threadEndTime) {
		this.migrators = migrators;
		this.threadEndTime = threadEndTime;
	}

	public DataMigrateRepairThread(IncrementDataMigrator migrator, long threadEndTime) {
		this.migrators = new ArrayList<>();
		this.migrators.add(migrator);
		this.threadEndTime = threadEndTime;
	}

	@Override
	public void run() {
		try {
			logger.info("--------->开始数据迁移任务，迁移任务数：" + migrators.size() + "个<---------");

			for (IncrementDataMigrator migrator : migrators) {

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

				MigrateResult result = migrator.migrateData(startTime, null, dataMigration.getMaximumMigrate());

				if (result != null) {
					migrator.setScheduleStartTime(result.getMigrateEndTime());
					SimpleDateFormat format = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm");
					logger.info("计划迁移数据任务[ID:" + id + "]执行" + (result.isSuccess() ? "完成" : "未完成") + "，迁移数据条数：" + result.getMigrateNum() + "条，迁移开始时间："
							+ format.format(result.getMigrateBeginTime()) + "，迁移结束时间：" + format.format(result.getMigrateEndTime()));
				} else {
					logger.info("计划迁移数据任务[ID:" + id + "]执行异常");
				}

				if (threadEndTime > 0 && threadEndTime < System.currentTimeMillis()) {
					break;
				}
			}

		} catch (Exception e) {
			logger.error("数据迁移异常", e);
		} finally {
			logger.info("--------->数据迁移任务结束<---------");
		}
	}

}
