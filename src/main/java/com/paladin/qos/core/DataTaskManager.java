package com.paladin.qos.core;

import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

public class DataTaskManager {

	@Value("${qos.data-task-pool-size}")
	private int dataTaskPoolSize = 50;

	private ExecutorService executorService;

	private CopyOnWriteArrayList<DataTask> dawnTasks;
	private CopyOnWriteArrayList<DataTask> nightTasks;
	private CopyOnWriteArrayList<DataTask> realTimeTasks;

	@PostConstruct
	public void init() {
		executorService = new ThreadPoolExecutor(dataTaskPoolSize, dataTaskPoolSize, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256), // 使用有界队列，避免OOM
				new ThreadPoolExecutor.DiscardPolicy());

		dawnTasks = new CopyOnWriteArrayList<>();
		nightTasks = new CopyOnWriteArrayList<>();
		realTimeTasks = new CopyOnWriteArrayList<>();
	}

	public void registerTaskBeforeDawn(List<DataTask> tasks) {
		dawnTasks.addAll(tasks);
	}

	public void registerTaskAtNight(List<DataTask> tasks) {
		nightTasks.addAll(tasks);
	}

	public void registerTaskRealTime(List<DataTask> tasks) {
		realTimeTasks.addAll(tasks);
	}

	/**
	 * 凌晨执行到5点
	 */
	@Scheduled(cron = "0 0 0 * * ?")
	public void executeScheduleDawn() {
		for (DataTask task : dawnTasks) {
			if (needScheduleToday(task)) {
				executorService.execute(task);
			}
		}
	}

	/**
	 * 夜晚7点执行到24点
	 */
	@Scheduled(cron = "0 0 19 * * ?")
	public void executeScheduleNight() {
		for (DataTask task : nightTasks) {
			if (needScheduleToday(task)) {
				executorService.execute(task);
			}
		}
	}

	@Scheduled(cron = "0 */1 * * * ?")
	public void executeRealTime() {
		for (DataTask task : realTimeTasks) {
			if (needRealTime(task)) {
				executorService.execute(task);
			}
		}
	}

	/**
	 * 是否需要实时
	 * 
	 * @param task
	 * @return
	 */
	public boolean needRealTime(DataTask task) {
		DataTaskConfiguration configuration = task.getConfiguration();
		return configuration.getRealTimeEnabled() == 1;
	}

	/**
	 * 每日调度任务时判断是否需要执行
	 * 
	 * @return
	 */
	public boolean needScheduleToday(DataTask task) {
		DataTaskConfiguration configuration = task.getConfiguration();
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

}
