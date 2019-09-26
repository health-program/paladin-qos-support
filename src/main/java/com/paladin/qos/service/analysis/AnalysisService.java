package com.paladin.qos.service.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.paladin.framework.core.exception.BusinessException;
import com.paladin.qos.analysis.DataByUnit;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.analysis.DataConstantContainer.Event;
import com.paladin.qos.analysis.DataConstantContainer.Unit;
import com.paladin.qos.analysis.DataProcessContainer;
import com.paladin.qos.analysis.DataProcessor;
import com.paladin.qos.analysis.TimeUtil;
import com.paladin.qos.mapper.analysis.AnalysisMapper;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.data.AnalysisMonth;
import com.paladin.qos.service.analysis.data.AnalysisUnit;
import com.paladin.qos.service.analysis.data.DataCountUnit;
import com.paladin.qos.service.analysis.data.DataPointDay;
import com.paladin.qos.service.analysis.data.DataPointMonth;
import com.paladin.qos.service.analysis.data.DataPointWeekMonth;
import com.paladin.qos.service.analysis.data.DataPointWeekYear;
import com.paladin.qos.service.analysis.data.DataPointYear;
import com.paladin.qos.service.analysis.data.DataResult;
import com.paladin.qos.service.analysis.data.TestResult;
import com.paladin.qos.service.analysis.data.ValidateEventResult;
import com.paladin.qos.service.analysis.data.ValidateUnitResult;
import com.paladin.qos.service.analysis.data.DataPointUnit;

@Service
public class AnalysisService {

	public final static int DATA_TYPE_DAY = 1;
	public final static int DATA_TYPE_MONTH = 2;
	public final static int DATA_TYPE_YEAR = 3;
	public final static int DATA_TYPE_WEEK_MONTH = 4;
	public final static int DATA_TYPE_WEEK_YEAR = 5;

	@Autowired
	private AnalysisMapper analysisMapper;

	@Autowired
	private DataProcessContainer dataProcessContainer;

	// ----------------------------------->查找某事件某医院在时间段内按时间粒度统计数据<-----------------------------------

	public DataPointUnit<DataPointDay> getDataPointUnitOfDay(String eventId, String unitId, Date startDate, Date endDate) {
		List<DataPointDay> points = analysisMapper.getDataPointOfDay(eventId, unitId, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		return new DataPointUnit<DataPointDay>(unitId, points);
	}

	public DataPointUnit<DataPointMonth> getDataPointUnitOfMonth(String eventId, String unitId, Date startDate, Date endDate) {
		List<DataPointMonth> points = analysisMapper.getDataPointOfMonth(eventId, unitId, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		return new DataPointUnit<DataPointMonth>(unitId, points);
	}

	public DataPointUnit<DataPointYear> getDataPointUnitOfYear(String eventId, String unitId, int startYear, int endYear) {
		List<DataPointYear> points = analysisMapper.getDataPointOfYear(eventId, unitId, startYear, endYear);
		return new DataPointUnit<DataPointYear>(unitId, points);
	}

	public DataPointUnit<DataPointWeekYear> getDataPointUnitOfWeekYear(String eventId, String unitId, int startYear, int endYear) {
		List<DataPointWeekYear> points = analysisMapper.getDataPointOfWeekYear(eventId, unitId, startYear, endYear);
		return new DataPointUnit<DataPointWeekYear>(unitId, points);
	}

	public DataPointUnit<DataPointWeekMonth> getDataPointUnitOfWeekMonth(String eventId, String unitId, Date startDate, Date endDate) {
		List<DataPointWeekMonth> points = analysisMapper.getDataPointOfWeekMonth(eventId, unitId, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		return new DataPointUnit<DataPointWeekMonth>(unitId, points);
	}

	// ----------------------------------->查找某事件单个或所有医院在时间段内按时间粒度统计<-----------------------------------

	private List<Unit> getUnitByType(int unitType) {
		if (unitType == DataUnit.TYPE_HOSPITAL) {
			return DataConstantContainer.getHospitalList();
		} else if (unitType == DataUnit.TYPE_COMMUNITY) {
			return DataConstantContainer.getCommunityList();
		}

		return DataConstantContainer.getUnitList();
	}

	// 查找多个或所有医院时，可改为在SQL中处理，暂时为遍历医院一个个查找

	/**
	 * 获取某事件在时间段内所有单位的日统计数据集合（按天统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointDay> getDataSetOfDay(String eventId, Date startDate, Date endDate) {
		// TODO 可优化为直接查数据库
		return getDataSetOfDay(eventId, DataConstantContainer.getUnitList(), startDate, endDate);
	}

	/**
	 * 获取某事件在时间段内某类型单位（医院、社区、站）的日统计数据集合（按天统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param unitType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointDay> getDataSetOfDay(String eventId, int unitType, Date startDate, Date endDate) {
		// TODO 可优化为直接查数据库，通过unit_type
		return getDataSetOfDay(eventId, getUnitByType(unitType), startDate, endDate);
	}

	/**
	 * 获取某事件在时间段内某些单位的日统计数据集合（按天统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param units
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointDay> getDataSetOfDay(String eventId, List<Unit> units, Date startDate, Date endDate) {
		if (units == null) {
			return null;
		}

		List<DataPointUnit<DataPointDay>> unitPoints = new ArrayList<>(units.size());
		for (Unit unit : units) {
			unitPoints.add(getDataPointUnitOfDay(eventId, unit.getId(), startDate, endDate));
		}

		return new DataResult<DataPointDay>(eventId, DATA_TYPE_DAY, unitPoints);
	}

	/**
	 * 获取某事件在时间段内所有单位的月统计数据集合（按月统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointMonth> getDataSetOfMonth(String eventId, Date startDate, Date endDate) {
		// TODO 可优化为直接查数据库
		return getDataSetOfMonth(eventId, DataConstantContainer.getUnitList(), startDate, endDate);
	}

	/**
	 * 获取某事件在时间段内某类型单位（医院、社区、站）的月统计数据集合（按月统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param unitType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointMonth> getDataSetOfMonth(String eventId, int unitType, Date startDate, Date endDate) {
		// TODO 可优化为直接查数据库
		return getDataSetOfMonth(eventId, getUnitByType(unitType), startDate, endDate);
	}

	/**
	 * 获取某事件在时间段内某些单位的月统计数据集合（按月统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param units
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointMonth> getDataSetOfMonth(String eventId, List<Unit> units, Date startDate, Date endDate) {
		if (units == null) {
			return null;
		}

		List<DataPointUnit<DataPointMonth>> unitPoints = new ArrayList<>(units.size());
		for (Unit unit : units) {
			unitPoints.add(getDataPointUnitOfMonth(eventId, unit.getId(), startDate, endDate));
		}
		return new DataResult<DataPointMonth>(eventId, DATA_TYPE_MONTH, unitPoints);
	}

	/**
	 * 获取某事件在时间段内所有单位的年统计数据集合（按年统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointYear> getDataSetOfYear(String eventId, int startYear, int endYear) {
		// TODO 可优化为直接查数据库
		return getDataSetOfYear(eventId, DataConstantContainer.getUnitList(), startYear, endYear);
	}

	/**
	 * 获取某事件在时间段内某类型单位（医院、社区、站）的年统计数据集合（按年统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param unitType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public DataResult<DataPointYear> getDataSetOfYear(String eventId, int unitType, int startYear, int endYear) {
		// TODO 可优化为直接查数据库
		return getDataSetOfYear(eventId, getUnitByType(unitType), startYear, endYear);
	}

	/**
	 * 获取某事件在时间段内某些单位的年统计数据集合（按年统计的数据集合，可用于按天统计的热力图、柱状图）
	 * 
	 * @param eventId
	 * @param units
	 * @param startYear
	 * @param endYear
	 * @return
	 */
	public DataResult<DataPointYear> getDataSetOfYear(String eventId, List<Unit> units, int startYear, int endYear) {
		if (units == null) {
			return null;
		}

		List<DataPointUnit<DataPointYear>> unitPoints = new ArrayList<>(units.size());
		for (Unit unit : units) {
			unitPoints.add(getDataPointUnitOfYear(eventId, unit.getId(), startYear, endYear));
		}
		return new DataResult<DataPointYear>(eventId, DATA_TYPE_YEAR, unitPoints);
	}

	/**
	 * 按所有单位分组获取时间段内某事件的发生概率
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<AnalysisUnit> getAnalysisResultByUnit(String eventId, Date startDate, Date endDate) {
		List<AnalysisUnit> result = analysisMapper.getAnalysisResultGroupByUnit(eventId, 0, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		orderByUnit(result);
		return result;
	}

	/**
	 * 按某类型单位分组获取时间段内某事件的发生概率
	 * 
	 * @param eventId
	 * @param unitType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<AnalysisUnit> getAnalysisResultByUnit(String eventId, int unitType, Date startDate, Date endDate) {
		List<AnalysisUnit> result = analysisMapper.getAnalysisResultGroupByUnit(eventId, unitType, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		orderByUnit(result);
		return result;
	}

	/**
	 * 按月分组获取时间段内所有单位某事件的发生概率
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<AnalysisMonth> getAnalysisResultByMonth(String eventId, Date startDate, Date endDate) {
		return analysisMapper.getAnalysisResultGroupByMonth(eventId, 0, TimeUtil.getSerialNumberByDay(startDate), TimeUtil.getSerialNumberByDay(endDate));
	}

	/**
	 * 按月分组获取时间段内所有单位某事件的发生概率
	 * 
	 * @param eventId
	 * @param unitType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<AnalysisMonth> getAnalysisResultByMonth(String eventId, int unitType, Date startDate, Date endDate) {
		return analysisMapper.getAnalysisResultGroupByMonth(eventId, unitType, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
	}

	/**
	 * 
	 * 按月分组获取时间段内某单位某事件的发生概率
	 * 
	 * @param eventId
	 * @param unitId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<AnalysisMonth> getAnalysisResultByMonth(String eventId, String unitId, Date startDate, Date endDate) {
		return analysisMapper.getAnalysisResultOfUnitGroupByMonth(eventId, unitId, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
	}

	/**
	 * 获取时间段内所有单位某事件的总数
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<DataCountUnit> countTotalNumByUnit(String eventId, Date startDate, Date endDate) {
		List<DataCountUnit> result = analysisMapper.countTotalNumByUnit(eventId, 0, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		orderByUnit(result);
		return result;
	}

	/**
	 * 获取时间段内某类型单位某事件的总数
	 * 
	 * @param eventId
	 * @param unitType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<DataCountUnit> countTotalNumByUnit(String eventId, int unitType, Date startDate, Date endDate) {
		List<DataCountUnit> result = analysisMapper.countTotalNumByUnit(eventId, unitType, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		orderByUnit(result);
		return result;
	}

	/**
	 * 获取时间段内所有单位某事件的事件总数
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<DataCountUnit> countEventNumByUnit(String eventId, Date startDate, Date endDate) {
		List<DataCountUnit> result = analysisMapper.countEventNumByUnit(eventId, 0, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		orderByUnit(result);
		return result;
	}

	/**
	 * 获取时间段内某类型单位某事件的事件总数
	 * 
	 * @param eventId
	 * @param unitType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<DataCountUnit> countEventNumByUnit(String eventId, int unitType, Date startDate, Date endDate) {
		List<DataCountUnit> result = analysisMapper.countEventNumByUnit(eventId, unitType, TimeUtil.getSerialNumberByDay(startDate),
				TimeUtil.getSerialNumberByDay(endDate));
		orderByUnit(result);
		return result;
	}

	/**
	 * 获取事件发生总数
	 * 
	 * @param eventId
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public long getTotalNumOfEvent(String eventId, Date startDate, Date endDate) {
		return analysisMapper.getTotalNumOfEvent(eventId, TimeUtil.getSerialNumberByDay(startDate), TimeUtil.getSerialNumberByDay(endDate));
	}

	// 单位排序
	private void orderByUnit(List<? extends DataByUnit> list) {
		if (list != null && list.size() > 0) {
			Collections.sort(list, new Comparator<DataByUnit>() {
				@Override
				public int compare(DataByUnit o1, DataByUnit o2) {
					String uid1 = o1.getUnitId();
					String uid2 = o1.getUnitId();
					return DataConstantContainer.getUnit(uid1).getOrderNum() > DataConstantContainer.getUnit(uid2).getOrderNum() ? 1 : -1;
				}
			});
		}

	}

	/**
	 * 检验时间段内缺失的数据
	 * 
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public List<ValidateEventResult> validateProcessedData(Date startTime, Date endTime) {

		if (startTime == null || endTime == null) {
			throw new BusinessException("请传入正确时间段");
		}

		List<Integer> serialNumbers = TimeUtil.getSerialNumberByDay(startTime, endTime);
		if (serialNumbers == null || serialNumbers.size() == 0) {
			throw new BusinessException("请传入正确时间段");
		}

		List<Event> events = DataConstantContainer.getEventList();
		List<ValidateEventResult> results = new ArrayList<>(events.size());

		for (Event event : events) {
			String eventId = event.getId();
			int targetType = event.getTargetType();

			List<Unit> units = DataConstantContainer.getUnitListByType(targetType);
			if (units == null) {
				return null;
			}

			List<ValidateUnitResult> unitResults = new ArrayList<>();
			for (Unit unit : units) {
				ValidateUnitResult unitResult = validateProcessedData(eventId, unit.getId(), serialNumbers);
				if (unitResult != null) {
					unitResults.add(unitResult);
				}
			}

			ValidateEventResult result = new ValidateEventResult();
			result.setEventId(eventId);
			result.setUnitResults(unitResults);

			results.add(result);
		}

		return results;
	}

	private ValidateUnitResult validateProcessedData(String eventId, String unitId, List<Integer> serialNumbers) {

		List<Integer> nums = analysisMapper.getSerialNumbers(eventId, unitId);

		ValidateUnitResult unitResult = new ValidateUnitResult();

		HashSet<Integer> numSet = new HashSet<>();

		if (nums != null) {
			for (Integer num : nums) {
				numSet.add(num);
			}
		}

		List<Integer> lostNums = new ArrayList<>();

		for (Integer sn : serialNumbers) {
			if (!numSet.contains(sn)) {
				lostNums.add(sn);
			}
		}

		if (nums != null && nums.size() > 0) {
			int first = nums.get(0);
			int last = nums.get(nums.size() - 1);
			unitResult.setFirstDay(first);
			unitResult.setLastDay(last);
		}

		unitResult.setLostDays(lostNums);
		unitResult.setUnitId(unitId);

		return unitResult;
	}

	/**
	 * 测试处理程序
	 * 
	 * @param event
	 * @return
	 */
	public TestResult testProcessor(Event event) {
		if (event != null) {
			String eventId = event.getId();

			TestResult result = new TestResult();
			result.setEventId(eventId);

			DataProcessor processor = dataProcessContainer.getDataProcessor(eventId);
			if (processor != null) {

				int targetType = event.getTargetType();
				List<Unit> units = DataConstantContainer.getUnitListByType(targetType);
				if (units == null) {
					return null;
				}

				Date startTime = TimeUtil.getTodayBefore(45);
				Date endTime = TimeUtil.getTodayBefore(30);

				long begin = System.currentTimeMillis();

				try {
					for (Unit unit : units) {
						String unitId = unit.getId();
						processor.getTotalNum(startTime, endTime, unitId);
						processor.getEventNum(startTime, endTime, unitId);
					}

					long castTime = System.currentTimeMillis() - begin;
					int processTimes = units.size();

					result.setCastTime(castTime);
					result.setProcessTimes(processTimes);
					result.setSuccess(true);
				} catch (Exception e) {
					result.setSuccess(false);
					result.setException(e.getMessage());
				}
			} else {
				result.setSuccess(false);
				result.setException("没有对应的DataProcessor实现类");
			}

			return result;
		}
		return null;
	}

	/**
	 * 测试所有处理程序
	 * 
	 * @return
	 */
	public List<TestResult> testProcessors() {
		List<Event> events = DataConstantContainer.getEventList();
		List<TestResult> results = new ArrayList<>(events.size());
		for (Event event : events) {

			TestResult result = testProcessor(event);
			if (result != null) {
				results.add(result);
			}
		}
		return results;
	}

	/**
	 * 获取事件、单位当前处理到的日期
	 * 
	 * @param eventId
	 * @return
	 */
	public Integer getCurrentDayOfEventAndUnit(String eventId, String unitId) {
		return analysisMapper.getMaxSerialNumByEventAndUnit(eventId, unitId);
	}

	/**
	 * 删除某天某事件数据
	 * 
	 * @param serialNumber
	 * @param eventId
	 * @return
	 */
	public int removeDataOfDay(int serialNumber, String eventId) {
		return analysisMapper.removeDataOfDay(serialNumber, eventId);
	}

}
