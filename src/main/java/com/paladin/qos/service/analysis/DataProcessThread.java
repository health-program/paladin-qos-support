package com.paladin.qos.service.analysis;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.qos.analysis.DataProcessor;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.util.TimeUtil;

public class DataProcessThread extends Thread {

	private static Logger logger = LoggerFactory.getLogger(DataProcessThread.class);

	private AnalysisService analysisService;
	private List<DataProcessor> processors;

	private Date startTime;
	private Date endTime;

	private boolean finished = false;
	private boolean shutdown = false;

	private int count = 0;
	private int total = 0;

	private Set<String> targetUnitIdSet = null;

	public DataProcessThread(AnalysisService analysisService, List<DataProcessor> processors, List<DataUnit> targetUnits, Date startTime, Date endTime) {
		this.processors = processors;
		this.analysisService = analysisService;
		this.startTime = TimeUtil.toDayTime(startTime);
		this.endTime = TimeUtil.toDayTime(endTime);

		int d = (int) ((endTime.getTime() - startTime.getTime()) / TimeUtil.MILLIS_IN_DAY);
		int total = 0;
		for (DataProcessor processor : processors) {
			total += d * processor.getTargetUnits().size();
		}
		this.total = total;

		if (targetUnits != null && targetUnits.size() > 0) {
			targetUnitIdSet = new HashSet<>();
			for (DataUnit unit : targetUnits) {
				targetUnitIdSet.add(unit.getId());
			}
		}

	}

	@Override
	public void run() {
		try {
			for (DataProcessor processor : processors) {

				List<DataUnit> units = processor.getTargetUnits();
				// 不能超过今天
				long end = endTime.getTime() > System.currentTimeMillis() ? TimeUtil.toDayTime(new Date()).getTime() : endTime.getTime();

				for (DataUnit unit : units) {
					String unitId = unit.getId();

					if (targetUnitIdSet != null && !targetUnitIdSet.contains(unitId)) {
						continue;
					}

					long start = startTime.getTime();

					int count = 0;
					while (start <= end) {
						Date startD = new Date(start);
						start += TimeUtil.MILLIS_IN_DAY;
						Date endD = new Date(start);

						boolean confirmed = true;
						boolean success = analysisService.processDataForOneDay(startD, endD, unitId, processor, confirmed);

						if (!success) {
							break;
						}

						count++;

						if (shutdown) {
							break;
						}
					}

					this.count += count;

					if (shutdown) {
						break;
					}
				}

				if (shutdown) {
					break;
				}
			}
		} catch (Exception e) {
			logger.error("处理数据异常", e);
		} finally {
			finished = true;
		}
	}

	public int getProcessedCount() {
		return count;
	}

	public boolean isFinished() {
		return finished;
	}

	public void shutdown() {
		this.shutdown = true;
	}

	public int getTotal() {
		return total;
	}
}
