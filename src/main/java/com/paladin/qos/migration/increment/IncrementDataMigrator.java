package com.paladin.qos.migration.increment;

import java.util.Date;

import com.paladin.qos.model.migration.DataMigration;

public interface IncrementDataMigrator {

	public String getId();

	/**
	 * 设置数据迁移描述
	 * 
	 * @param dataMigration
	 */
	public void setDataMigration(DataMigration dataMigration);

	/**
	 * 获取数据迁移描述
	 * 
	 * @return
	 */
	public DataMigration getDataMigration();

	/**
	 * 迁移数据
	 * 
	 * @param updateStartTime
	 *            开始迁移数据时间
	 * @param updateEndTime
	 *            结束迁移数据时间，为null则为开始时间后所有
	 * @param migrateNum
	 *            迁移数据最大量
	 * @return
	 */
	public MigrateResult migrateData(Date updateStartTime, Date updateEndTime, int migrateNum);

	/**
	 * 获取计划迁移数据开始时间
	 * 
	 * @return
	 */
	public Date getScheduleStartTime();

	/**
	 * 设置调度开始时间
	 * 
	 * @param scheduleStartTime
	 */
	public void setScheduleStartTime(Date scheduleStartTime);

	/**
	 * 调度数据归档时间，处于归档时间之后的数据将会在下次调度任务中被重写
	 * 
	 * @return
	 */
	public Date getScheduleFilingDate();

	/**
	 * 每日调度任务时判断是否需要执行
	 * 
	 * @return
	 */
	default public boolean needScheduleToday() {
		return true;
	}

	/**
	 * 迁移结果
	 * 
	 * @author TontoZhou
	 * @since 2019年10月10日
	 */
	public static class MigrateResult {

		private boolean success;
		private int migrateNum;
		private Date migrateBeginTime;
		private Date migrateEndTime;

		public MigrateResult(Date migrateBeginTime) {
			this.success = false;
			this.migrateNum = 0;
			this.migrateBeginTime = migrateBeginTime;
			this.migrateEndTime = migrateBeginTime;
		}

		public MigrateResult(boolean success, int migrateNum, Date migrateBeginTime, Date migrateEndTime) {
			this.success = success;
			this.migrateNum = migrateNum;
			this.migrateBeginTime = migrateBeginTime;
			this.migrateEndTime = migrateEndTime;
		}

		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public int getMigrateNum() {
			return migrateNum;
		}

		public void setMigrateNum(int migrateNum) {
			this.migrateNum = migrateNum;
		}

		public Date getMigrateBeginTime() {
			return migrateBeginTime;
		}

		public void setMigrateBeginTime(Date migrateBeginTime) {
			this.migrateBeginTime = migrateBeginTime;
		}

		public Date getMigrateEndTime() {
			return migrateEndTime;
		}

		public void setMigrateEndTime(Date migrateEndTime) {
			this.migrateEndTime = migrateEndTime;
		}

	}

}
