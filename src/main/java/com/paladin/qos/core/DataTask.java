package com.paladin.qos.core;

import java.util.Calendar;
import java.util.Date;

import com.paladin.qos.util.TimeUtil;

public abstract class DataTask implements Runnable {

	private String id;
	private DataTaskConfiguration configuration;
	private long threadEndTime;

	protected volatile long realTimeMillisecond;
	private boolean realTime;
	private long realTimeIntervalMillisecond;

	public DataTask(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setConfiguration(DataTaskConfiguration configuration) {
		this.configuration = configuration;
		this.realTime = configuration.getRealTimeEnabled() == 1;
		if (realTime) {
			realTimeMillisecond = System.currentTimeMillis();
			realTimeIntervalMillisecond = configuration.getRealTimeInterval() * 60 * 1000;
		}
	}

	public DataTaskConfiguration getConfiguration() {
		return configuration;
	}

	// --------------------------------------------------------------
	//
	// 如果任务不能同时重复执行，则可以通过获取锁，释放锁操作
	//
	// --------------------------------------------------------------

	private volatile Boolean lock = false;

	private boolean getLock() {
		synchronized (lock) {
			if (lock) {
				return false;
			} else {
				lock = true;
				return true;
			}
		}
	}

	private void cancelLock() {
		synchronized (lock) {
			lock = false;
		}
	}

	public boolean isRun() {
		return lock;
	}

	public void run() {
		if (getLock()) {
			try {
				doTask();
				if (realTime) {
					realTimeMillisecond = System.currentTimeMillis();
				}
			} finally {
				cancelLock();
			}
		}
	}

	public boolean isEnabled() {
		return configuration.getEnabled() == 1;
	}

	public boolean isRealTime() {
		return realTime;
	}

	public boolean doRealTime() {
		return System.currentTimeMillis() - realTimeMillisecond >= realTimeIntervalMillisecond;
	}

	public boolean isThreadFinished() {
		return threadEndTime > 0 && threadEndTime < System.currentTimeMillis();
	}

	public abstract void doTask();
	
	/**
	 * 根据归档策略获取归档的最终数据时间
	 * 
	 * @return
	 */
	public Date getScheduleFilingDate() {
		int filingStrategy = configuration.getFilingStrategy();
		if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_UNTIL_NOW) {
			return null;
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_UNTIL_DAY) {
			return TimeUtil.getTodayBefore(configuration.getFilingStrategyParam1());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_UNTIL_MONTH) {
			return TimeUtil.getTodayBeforeMonth(configuration.getFilingStrategyParam1());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_UNTIL_YEAR) {
			return TimeUtil.getTodayBeforeYear(configuration.getFilingStrategyParam1());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_FIXED_DAY_OF_MONTH) {
			int day = configuration.getFilingStrategyParam1();
			Calendar c = Calendar.getInstance();
			int currentDay = c.get(Calendar.DAY_OF_MONTH);
			if (currentDay > day) {
				// 如果超过归档日，则应该归档到上月底
				c.set(Calendar.DAY_OF_MONTH, 1);

			} else {
				// 上月归档日前一天
				c.set(Calendar.DAY_OF_MONTH, 1);
				c.add(Calendar.MONTH, -1);
			}
			c.add(Calendar.DAY_OF_MONTH, -1);
			return TimeUtil.toDayTime(c.getTime());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_FIXED_DAY_OF_YEAR) {
			// 归档日为每年某天
			String dayStr = configuration.getFilingStrategyParam2();
			String[] ss = dayStr.split("/");
			int m = Integer.valueOf(ss[0]);
			int d = Integer.valueOf(ss[1]);

			Calendar c = Calendar.getInstance();

			int cm = c.get(Calendar.MONTH) + 1;
			int cd = c.get(Calendar.DAY_OF_MONTH);

			if (cm > m || (cm == m && cd > d)) {
				c.set(Calendar.MONTH, 0);
				c.set(Calendar.DAY_OF_MONTH, 1);
			} else {
				c.set(Calendar.MONTH, 0);
				c.set(Calendar.DAY_OF_MONTH, 1);
				c.add(Calendar.YEAR, -1);
			}

			c.add(Calendar.DAY_OF_MONTH, -1);
			return TimeUtil.toDayTime(c.getTime());
		} else if (filingStrategy == DataTaskConfiguration.FILING_STRATEGY_CUSTOM) {

		}

		throw new RuntimeException("还未实现归档策略：" + filingStrategy);
	}

	/**
	 * 每日调度任务时判断是否需要执行
	 * 
	 * @return
	 */
	public boolean needScheduleToday() {
		int scheduleStrategy = configuration.getScheduleStrategy();
		if (scheduleStrategy == DataTaskConfiguration.SCHEDULE_STRATEGY_NO) {
			return false;
		} else if (scheduleStrategy == DataTaskConfiguration.SCHEDULE_STRATEGY_EVERY_DAY) {
			return true;
		} else if (scheduleStrategy == DataTaskConfiguration.SCHEDULE_STRATEGY_FIXED_DAY_OF_MONTH) {
			String dayStr = configuration.getScheduleStrategyParam2();
			String[] days = dayStr.split(",");
			Calendar c = Calendar.getInstance();
			String d = String.valueOf(c.get(Calendar.DAY_OF_MONTH));
			for (String day : days) {
				if (d.equals(day)) {
					return true;
				}
			}
			return false;
		} else if (scheduleStrategy == DataTaskConfiguration.SCHEDULE_STRATEGY_FIXED_DAY_OF_YEAR) {
			String dayStr = configuration.getScheduleStrategyParam2();
			String[] days = dayStr.split(",");
			Calendar c = Calendar.getInstance();
			String d = String.valueOf(c.get(Calendar.MONTH) + 1) + "/" + String.valueOf(c.get(Calendar.DAY_OF_MONTH));
			for (String day : days) {
				if (d.equals(day)) {
					return true;
				}
			}
			return false;
		} else if (scheduleStrategy == DataTaskConfiguration.SCHEDULE_STRATEGY_CUSTOM) {

		}

		throw new RuntimeException("还未实现策略：" + scheduleStrategy);
	}

	public synchronized void setThreadEndTime(long threadEndTime) {
		this.threadEndTime = threadEndTime;
	}

	public long getThreadEndTime() {
		return threadEndTime;
	}

	public long getRealTimeMillisecond() {
		return realTimeMillisecond;
	}

	public long getRealTimeIntervalMillisecond() {
		return realTimeIntervalMillisecond;
	}
}
