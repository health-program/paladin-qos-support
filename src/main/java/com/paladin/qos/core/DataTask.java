package com.paladin.qos.core;

import java.util.Calendar;
import java.util.Date;

import com.paladin.qos.model.migration.DataMigration;
import com.paladin.qos.util.TimeUtil;

public abstract class DataTask implements Runnable {

	private String id;
	private DataTaskConfiguration configuration;
	private long threadEndTime;

	protected volatile long realTimeMillisecond;
	private boolean realTime;
	private long realTimeIntervalMillisecond;

	public DataTask(DataTaskConfiguration configuration, long threadEndTime) {
		this.configuration = configuration;
		this.id = configuration.getId();
		this.threadEndTime = threadEndTime;
		this.realTime = configuration.getRealTimeEnabled() == 1;
		if (realTime) {
			realTimeMillisecond = System.currentTimeMillis();
			realTimeIntervalMillisecond = configuration.getRealTimeInterval() * 60 * 1000;
		}
	}

	public String getId() {
		return id;
	}

	public DataTaskConfiguration getConfiguration() {
		return configuration;
	}

	// --------------------------------------------------------------
	//
	// 如果任务不能同时重复执行，则可以通过获取锁，释放锁操作
	//
	// --------------------------------------------------------------

	private Boolean lock = false;

	public boolean getLock() {
		synchronized (lock) {
			if (lock) {
				return false;
			} else {
				lock = true;
				return true;
			}
		}
	}

	public void cancelLock() {
		synchronized (lock) {
			lock = false;
		}
	}

	public void run() {
		try {
			if (getLock()) {
				doTask();
				if (realTime) {
					realTimeIntervalMillisecond = System.currentTimeMillis();
				}
			}
		} finally {
			cancelLock();
		}
	}

	public boolean doRealTime() {
		return realTime && System.currentTimeMillis() - realTimeMillisecond < realTimeIntervalMillisecond;
	}

	public boolean isThreadFinished() {
		return threadEndTime > 0 && threadEndTime < System.currentTimeMillis();
	}

	public abstract void doTask();

	public Date getScheduleFilingDate() {
		int filingStrategy = configuration.getFilingStrategy();
		if (filingStrategy == DataMigration.FILING_STRATEGY_DEFAULT_NOW) {
			return null;
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_UNTIL_DAY) {
			return TimeUtil.getTodayBefore(configuration.getFilingStrategyParam1());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_UNTIL_MONTH) {
			return TimeUtil.getTodayBeforeMonth(configuration.getFilingStrategyParam1());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_UNTIL_YEAR) {
			return TimeUtil.getTodayBeforeYear(configuration.getFilingStrategyParam1());
		} else if (filingStrategy == DataTaskConfiguration.SCHEDULE_STRATEGY_FIXED_DAY_OF_MONTH) {
			int day = configuration.getFilingStrategyParam1();
			Calendar c = Calendar.getInstance();
			int currentDay = c.get(Calendar.DAY_OF_MONTH);
			if (currentDay > day) {
				// 该月归档日
				c.set(Calendar.DAY_OF_MONTH, day);
			} else {
				// 上月归档日前一天
				c.set(Calendar.DAY_OF_MONTH, day);
				c.add(Calendar.MONTH, -1);
			}
			c.add(Calendar.DAY_OF_MONTH, -1);
			return TimeUtil.toDayTime(c.getTime());
		} else if (filingStrategy == DataTaskConfiguration.SCHEDULE_STRATEGY_FIXED_DAY_OF_YEAR) {
			// 归档日为每年某天
			String dayStr = configuration.getFilingStrategyParam2();
			String[] ss = dayStr.split("/");
			int m = Integer.valueOf(ss[0]);
			int d = Integer.valueOf(ss[1]);

			Calendar c = Calendar.getInstance();

			int cm = c.get(Calendar.MONTH) + 1;
			int cd = c.get(Calendar.DAY_OF_MONTH);

			if (cm > m || (cm == m && cd > d)) {
				c.set(Calendar.MONTH, m - 1);
				c.set(Calendar.DAY_OF_MONTH, d);
			} else {
				c.set(Calendar.MONTH, m - 1);
				c.set(Calendar.DAY_OF_MONTH, d);
				c.add(Calendar.YEAR, -1);
			}

			c.add(Calendar.DAY_OF_MONTH, -1);
			return TimeUtil.toDayTime(c.getTime());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_CUSTOM) {

		}

		throw new RuntimeException("还未实现归档策略：" + filingStrategy);
	}
}
