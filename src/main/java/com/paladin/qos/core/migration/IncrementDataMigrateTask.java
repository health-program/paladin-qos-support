package com.paladin.qos.core.migration;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.migration.IncrementDataMigrator.MigrateResult;
import com.paladin.qos.model.migration.DataMigration;

public class IncrementDataMigrateTask extends DataTask {

	private static Logger logger = LoggerFactory.getLogger(IncrementDataMigrateTask.class);

	private IncrementDataMigrator dataMigrator;
	protected volatile Date updateTime;

	public IncrementDataMigrateTask(IncrementDataMigrator dataMigrator) {
		super(dataMigrator.getId());
		setConfiguration(dataMigrator.getDataMigration());
		this.dataMigrator = dataMigrator;
	}

	@Override
	public void doTask() {
		try {			
			DataMigration dataMigration = dataMigrator.getDataMigration();

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

			Date logStartTime = updateTime;

			do {
				result = dataMigrator.migrateData(updateTime, null, selectLimit);
				updateTime = result.getMigrateEndTime();

				int num = result.getMigrateNum();
				count += num;

				if (!result.isSuccess() || num < selectLimit || (maximumMigrate > 0 && count >= maximumMigrate)) {
					break;
				}

				if (!isRealTime() && isThreadFinished()) {
					break;
				}

			} while (true);

			SimpleDateFormat format = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm.sss");

			logger.info("迁移数据任务[ID:" + getId() + "]执行" + (result.isSuccess() ? "完成" : "未完成") + "，迁移数据条数：" + count + "条，迁移开始时间："
					+ (logStartTime == null ? "null" : format.format(logStartTime)) + "，迁移结束时间：" + (updateTime == null ? "null" : format.format(updateTime)));

		} catch (Exception e) {
			logger.error("数据迁移异常[ID:" + getId() + "]", e);
		}
	}

}
