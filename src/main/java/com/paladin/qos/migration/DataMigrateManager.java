package com.paladin.qos.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.paladin.qos.migration.increment.IncrementDataMigrator;
import com.paladin.qos.model.migration.DataMigration;

@Component
public class DataMigrateManager {

	private static Logger logger = LoggerFactory.getLogger(DataMigrateManager.class);

	@Autowired
	private DataMigratorContainer dataMigratorContainer;

	@Value("${qos.simple-mode}")
	private boolean simpleMode = false;

	// 线程池
	private ExecutorService executorService = new ThreadPoolExecutor(30, 30, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256), // 使用有界队列，避免OOM
			new ThreadPoolExecutor.DiscardPolicy());

	@Scheduled(cron = "0 0 19 * * ?")
	public void processUpdate() {
		if (simpleMode) {
			return;
		}

		logger.info("开始执行定时数据迁移任务");

		// 根据定时时间修改，例如晚上10点到明天凌晨5点，则加7个小时时间
		long threadEndTime = System.currentTimeMillis() + 10 * 60 * 60 * 1000;

		List<IncrementDataMigrator> migrators = dataMigratorContainer.getIncrementDataMigratorList();

		Map<String, List<IncrementDataMigrator>> migratorMap = new HashMap<>();

		for (IncrementDataMigrator migrator : migrators) {
			DataMigration dataMigration = migrator.getDataMigration();

			// 实时获取数据的单独处理
			if (dataMigration.getRealTimeEnabled() == 1) {
				continue;
			}

			if (dataMigration.getSeparateProcessThread() == 1) {
				executorService.execute(new DataMigrateRepairThread(new DataMigratorStack(migrator), threadEndTime));
			} else {
				String originDS = dataMigration.getOriginDataSource();
				List<IncrementDataMigrator> migratorList = migratorMap.get(originDS);
				if (migratorList == null) {
					migratorList = new ArrayList<>();
					migratorMap.put(originDS, migratorList);
				}
				migratorList.add(migrator);
			}
		}

		List<Stack> stacks = new ArrayList<>();
		for (Entry<String, List<IncrementDataMigrator>> entry : migratorMap.entrySet()) {
			Stack stack = new Stack(entry.getValue());
			stacks.add(stack);
		}

		for (int i = 0; i < stacks.size(); i++) {
			executorService.execute(new DataMigrateRepairThread(new DataMigratorStack(stacks, i), threadEndTime));
		}

	}

	@Scheduled(cron = "0 */5 * * * ?")
	public void processRealTime() {
		if (simpleMode) {
			return;
		}

		List<IncrementDataMigrator> migrators = dataMigratorContainer.getIncrementDataMigratorList();
		Map<String, List<IncrementDataMigrator>> migratorMap = new HashMap<>();

		for (IncrementDataMigrator migrator : migrators) {
			DataMigration dataMigration = migrator.getDataMigration();

			// 实时获取数据的单独处理
			if (dataMigration.getRealTimeEnabled() != 1) {
				continue;
			}

			if (dataMigration.getSeparateProcessThread() == 1) {
				executorService.execute(new DataMigrateRealTimeThread(new DataMigratorStack(migrator)));
			} else {
				String originDS = dataMigration.getOriginDataSource();
				List<IncrementDataMigrator> migratorList = migratorMap.get(originDS);
				if (migratorList == null) {
					migratorList = new ArrayList<>();
					migratorMap.put(originDS, migratorList);
				}
				migratorList.add(migrator);
			}
		}

		List<Stack> stacks = new ArrayList<>();
		for (Entry<String, List<IncrementDataMigrator>> entry : migratorMap.entrySet()) {
			Stack stack = new Stack(entry.getValue());
			stacks.add(stack);
		}

		for (int i = 0; i < stacks.size(); i++) {
			executorService.execute(new DataMigrateRealTimeThread(new DataMigratorStack(stacks, i)));
		}
	}

	/*
	 * Stack 和 DataMigratorStack 配合作为调用数据迁移任务的栈，基于优先同一数据源下迁移数据在同一个线程下，
	 * 迁移任务完成后再尝试去获取其他数据源下的迁移任务，并且多个线程切换数据源任务时都能切换到不同数据源，达到充分利用
	 * 每个数据源的性能，并同时利用到自身系统的性能。
	 * 
	 */

	private static class Stack {

		private int index;
		private List<IncrementDataMigrator> migrators;

		private Stack(List<IncrementDataMigrator> migrators) {
			this.migrators = migrators;
			this.index = migrators.size() - 1;
		}

		public synchronized IncrementDataMigrator pop() {
			if (index == -1) {
				return null;
			}

			IncrementDataMigrator result = migrators.get(index);
			index--;
			return result;
		}
	}

	public static class DataMigratorStack {

		private List<Stack> stacks;
		private int startIndex;
		private int visitCount;

		private IncrementDataMigrator migrator;
		private boolean singlePoped = false;

		private DataMigratorStack(List<Stack> stacks, int startIndex) {
			this.stacks = stacks;
			this.startIndex = startIndex;
			this.visitCount = 0;
		}

		private DataMigratorStack(IncrementDataMigrator migrator) {
			this.migrator = migrator;
		}

		public IncrementDataMigrator pop() {
			if (migrator != null) {
				if (singlePoped) {
					return null;
				} else {
					singlePoped = true;
					return migrator;
				}
			}

			while (visitCount < stacks.size()) {
				IncrementDataMigrator result = stacks.get(startIndex).pop();
				if (result == null) {
					startIndex++;
					visitCount++;
					if (startIndex == stacks.size()) {
						startIndex = 0;
					}
				} else {
					return result;
				}
			}
			return null;
		}

	}

}
