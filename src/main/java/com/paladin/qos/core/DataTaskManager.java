package com.paladin.qos.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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

	@Value("${qos.simple-mode}")
	private boolean simpleMode = false;

	private ExecutorService executorService;

	private volatile List<DataTask> scheduleTasks = new ArrayList<>();
	private Map<String, DataTask> scheduleTaskMap = new ConcurrentHashMap<>();

	private volatile List<DataTask> realTimeTasks = new ArrayList<>();
	private Map<String, DataTask> realTimeTaskMap = new ConcurrentHashMap<>();

	@PostConstruct
	public void init() {
		executorService = new ThreadPoolExecutor(dataTaskPoolSize, dataTaskPoolSize, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(512), // 使用有界队列，避免OOM
				new ThreadPoolExecutor.DiscardPolicy());
	}

	public void registerTaskSchedule(List<DataTask> tasks) {
		for (DataTask task : tasks) {
			scheduleTaskMap.put(task.getId(), task);
		}
		scheduleTasks = new ArrayList<>(scheduleTaskMap.values());
	}

	public void registerTaskRealTime(List<DataTask> tasks) {
		realTimeTasks.addAll(tasks);
		for (DataTask task : tasks) {
			realTimeTaskMap.put(task.getId(), task);
		}
		realTimeTasks = new ArrayList<>(realTimeTaskMap.values());
	}

	/**
	 * 每小时执行
	 */
	@Scheduled(cron = "0 0 0/1 * * ?")
	public void executeScheduleNight() {
		if (simpleMode) {
			return;
		}

		for (DataTask task : scheduleTasks) {
			if (task.isEnabled() && !task.isRun() && task.needScheduleNow()) {
				long threadEndTime = System.currentTimeMillis() + task.getExecuteHours() * 60 * 60 * 1000;
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
		if (simpleMode) {
			return;
		}

		for (DataTask task : realTimeTasks) {
			boolean execute = task.isEnabled() && !task.isRun() && task.isRealTime() && task.doRealTime();
			if (execute) {
				executorService.execute(task);
			}
		}
	}

	public boolean executeTask(String taskId, long threadEndTime) {
		DataTask task = getTask(taskId);
		if (task != null && task.isEnabled() && !task.isRun()) {
			task.setThreadEndTime(threadEndTime);
			executorService.execute(task);
			return true;
		}
		return false;
	}

	public List<DataTask> getNightTasks() {
		return scheduleTasks;
	}

	public List<DataTask> getRealTimeTasks() {
		return realTimeTasks;
	}

	public DataTask getTask(String id) {

		for (DataTask task : scheduleTasks) {
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
