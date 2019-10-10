package com.paladin.qos.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.paladin.qos.migration.increment.IncrementDataMigrator;
import com.paladin.qos.model.migration.DataMigration;

@Component
public class DataMigrateManager {

	private static Logger logger = LoggerFactory.getLogger(DataMigrateManager.class);

	@Autowired
	private DataMigratorContainer dataMigratorContainer;

	// 线程池
	private ExecutorService executorService = new ThreadPoolExecutor(30, 30, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256), // 使用有界队列，避免OOM
			new ThreadPoolExecutor.DiscardPolicy());

	@Scheduled(cron = "0 0 19 * * ?")
	public void processUpdate() {

		logger.info("开始执行定时数据迁移任务");
		
		// 根据定时时间修改，例如晚上10点到明天凌晨5点，则加7个小时时间
		long threadEndTime = System.currentTimeMillis() + 10 * 60 * 60 * 1000;

		List<IncrementDataMigrator> migrators = dataMigratorContainer.getIncrementDataMigratorList();

		Map<String, List<IncrementDataMigrator>> migratorMap = new HashMap<>();

		for (IncrementDataMigrator migrator : migrators) {

			DataMigration dataMigration = migrator.getDataMigration();

			if (dataMigration.getSeparateProcessThread() == 1) {
				executorService.execute(new DataMigrateRepairThread(migrator, threadEndTime));
			} else {
				String originDS = dataMigration.getOriginDataSource();
				List<IncrementDataMigrator> migratorList = migratorMap.get(originDS);
				if (migratorList == null) {
					migratorList = new ArrayList<>();
					migratorMap.put(originDS, migratorList);
				} else {
					if (migratorList.size() == 8) {
						executorService.execute(new DataMigrateRepairThread(migratorList, threadEndTime));
						migratorList = new ArrayList<>();
						migratorMap.put(originDS, migratorList);
					}
				}
				migratorList.add(migrator);
			}
		}

		for (List<IncrementDataMigrator> ms : migratorMap.values()) {
			executorService.execute(new DataMigrateRepairThread(ms, threadEndTime));
		}
	}
	
	
	
}
