package com.paladin.qos.core.migration;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.migration.IncrementDataMigrator.MigrateResult;
import com.paladin.qos.model.migration.DataMigration;

public class IncrementDataMigrateTask extends DataTask {

	private static Logger logger = LoggerFactory.getLogger(IncrementDataMigrateTask.class);

	private IncrementDataMigrator dataMigrator;

	private Date migrateTime;
	private int updatedNum;
	private int insertedNum;

	protected volatile Date updateTime;

	public IncrementDataMigrateTask(IncrementDataMigrator dataMigrator) {
		super(dataMigrator.getId(), DataTask.LEVEL_MAJOR);
		DataMigration migration = dataMigrator.getDataMigration();

		setConfiguration(migration);
		List<Object> labels = new ArrayList<>();
		labels.add(migration.getName());
		labels.add(migration.getOriginDataSource());
		this.setLabels(labels);

		this.dataMigrator = dataMigrator;
	}

	@Override
	public void doTask() {
		try {
			migrateTime = new Date();
			updatedNum = 0;
			insertedNum = 0;

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

			MigrateResult result = null;

			Date logStartTime = updateTime;

			do {
				result = dataMigrator.migrateData(updateTime, null, selectLimit);
				updateTime = result.getMigrateEndTime();

				int updateN = result.getUpdatedNum();
				int insertN = result.getInsertedNum();

				updatedNum += updateN;
				insertedNum += insertN;

				int num = updateN + insertN;

				if (!result.isSuccess() || num < selectLimit || (maximumMigrate > 0 && (updatedNum + insertedNum) >= maximumMigrate)) {
					break;
				}

				if (!isRealTime() && isThreadFinished()) {
					break;
				}

			} while (true);

			SimpleDateFormat format = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:mm:ss.sss");

			logger.info("迁移数据任务[ID:" + getId() + "]执行" + (result.isSuccess() ? "完成" : "未完成") + "，迁移数据条数[更新：" + updatedNum + "条，新增：" + insertedNum + "条]，迁移开始时间："
					+ (logStartTime == null ? "null" : format.format(logStartTime)) + "，迁移结束时间：" + (updateTime == null ? "null" : format.format(updateTime)));

		} catch (Exception e) {
			logger.error("数据迁移异常[ID:" + getId() + "]", e);
		}
	}

	@Override
	public String getExecuteSituation() {
		if (updateTime == null) {
			return "还未执行";
		} else {
			SimpleDateFormat format = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:mm:ss");
			return "在" + format.format(migrateTime) + "增量迁移数据[更新：" + updatedNum + "条，新增：" + insertedNum + "条]条，当前增量更新时间为：" + format.format(updateTime);
		}
	}

}
