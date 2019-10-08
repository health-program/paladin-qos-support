package com.paladin.qos.migration;

import java.util.Date;

public interface DataMigrator {

	public MigrateResult migrateData(Date updateTime);

	public static class MigrateResult {

		private boolean success;
		private int migrateNum;
		private Date migrateBeginTime;
		private Date migrateEndTime;

		public MigrateResult(Date migrateBeginTime) {
			this.success = true;
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
