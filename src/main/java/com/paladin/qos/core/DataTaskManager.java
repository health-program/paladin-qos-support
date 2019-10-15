package com.paladin.qos.core;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DataTaskManager {

	@Value("${qos.data-task-pool-size}")
	private int dataTaskPoolSize = 50;

	private ExecutorService executorService;

	private CopyOnWriteArrayList<DataTask> dawnTasks = new CopyOnWriteArrayList<>();
	private CopyOnWriteArrayList<DataTask> nightTasks = new CopyOnWriteArrayList<>();
	private CopyOnWriteArrayList<DataTask> realTimeTasks = new CopyOnWriteArrayList<>();

	@PostConstruct
	public void init() {
		executorService = new ThreadPoolExecutor(dataTaskPoolSize, dataTaskPoolSize, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(512), // 使用有界队列，避免OOM
				new ThreadPoolExecutor.DiscardPolicy());
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
	 * 凌晨0点执行到5点
	 */
	@Scheduled(cron = "0 0 0 * * ?")
	public void executeScheduleDawn() {
		long threadEndTime = System.currentTimeMillis() + 5 * 60 * 60 * 1000;

		for (DataTask task : dawnTasks) {
			if (task.needScheduleToday()) {
				task.setThreadEndTime(threadEndTime);
				executorService.execute(task);
			}
		}
	}

	/**
	 * 夜晚7点执行到第二日凌晨5点
	 */
	@Scheduled(cron = "0 0 19 * * ?")
	public void executeScheduleNight() {
		long threadEndTime = System.currentTimeMillis() + 10 * 60 * 60 * 1000;

		for (DataTask task : nightTasks) {
			if (task.needScheduleToday()) {
				task.setThreadEndTime(threadEndTime);
				executorService.execute(task);
			}
		}
	}

	/**
	 * 每分钟尝试去执行
	 */
	@Scheduled(cron = "0 */1 * * * ?")
	public void executeRealTime() {
		for (DataTask task : realTimeTasks) {
			boolean execute = false;
			try {
				if (task.getLock()) {
					execute = task.isRealTime() && task.doRealTime();
				}
			} finally {
				task.cancelLock();
			}

			if (execute) {
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

	public CopyOnWriteArrayList<DataTask> getDawnTasks() {
		return dawnTasks;
	}

	public CopyOnWriteArrayList<DataTask> getNightTasks() {
		return nightTasks;
	}

	public CopyOnWriteArrayList<DataTask> getRealTimeTasks() {
		return realTimeTasks;
	}

	public DataTask getTask(String id) {

		for (DataTask task : dawnTasks) {
			if (task.getId().equals(id)) {
				return task;
			}
		}

		for (DataTask task : nightTasks) {
			if (task.getId().equals(id)) {
				return task;
			}
		}

		for (DataTask task : realTimeTasks) {
			if (task.getId().equals(id)) {
				return task;
			}
		}

		return null;
	}

}
