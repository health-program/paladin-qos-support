package com.paladin.qos.core.migration;

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
	 * 获取当前更新时间
	 * 
	 * @return
	 */
	public Date getCurrentUpdateTime();
	
	/**
	 * 迁移结果
	 * 
	 * @author TontoZhou
	 * @since 2019年10月10日
	 */
	public static class MigrateResult {

		private boolean success;
		private int updatedNum;
		private int insertedNum;
		private Date migrateBeginTime;
		private Date migrateEndTime;

		public MigrateResult(Date migrateBeginTime) {
			this.success = true;
			this.migrateBeginTime = migrateBeginTime;
			this.migrateEndTime = migrateBeginTime;
		}


		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
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

		public int getUpdatedNum() {
			return updatedNum;
		}

		public void setUpdatedNum(int updatedNum) {
			this.updatedNum = updatedNum;
		}

		public int getInsertedNum() {
			return insertedNum;
		}

		public void setInsertedNum(int insertedNum) {
			this.insertedNum = insertedNum;
		}

	}



}
