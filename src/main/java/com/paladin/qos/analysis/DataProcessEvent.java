package com.paladin.qos.analysis;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.paladin.qos.analysis.DataProcessUnit;

public class DataProcessEvent {

	public static final int EVENT_TYPE_RATE = 1;
	public static final int EVENT_TYPE_COUNT = 2;

	public static final int TARGET_TYPE_ALL = 1;
	public static final int TARGET_TYPE_HOSPITAL = 2;
	public static final int TARGET_TYPE_COMMUNITY = 3;

	public static final int SQL_SPEED_QUICK = 1; // 快，小于1分钟
	public static final int SQL_SPEED_SLOW = 2; // 慢， 1-2分钟
	public static final int SQL_SPEED_VERY_SLOW = 3; // 非常慢 大于2分钟

	/** 前多少天 */
	public final static int PROCESS_BEFORE_TYPE_DAY = 1;
	/** 前多少月 */
	public final static int PROCESS_BEFORE_TYPE_MONTH = 2;
	/** 前多少年 */
	public final static int PROCESS_BEFORE_TYPE_YEAR = 3;

	/** 过了某日，则读取到上一个月，如果没过，则读取到上上个月 */
	public final static int PROCESS_BEFORE_TYPE_SPECIAL_ONE = 4;

	private String id;
	private String name;
	// 事件类型，概率、总数
	private int eventType;
	// 目标单位类型
	private int targetType;
	// 涉及到的数据源
	private String dataSource;
	// 数据处理开始时间
	private Date processStartDate;
	// 与processBeforeType配合标明数据处理到哪个时间点，一般为相对于今天的前几天或几月
	private int processBefore;
	// 与processBeforeType配合标明数据处理到哪个时间点，一般为相对于今天的前几天或几月
	private int processBeforeType;
	// 是否实时
	private boolean realTimeEnabled;
	// 实时间隔
	private int realTimeInterval;
	// 数据处理SQL执行速度
	private int sqlSpeed;
	// 是否单独线程执行数据处理
	private boolean separateProcessThread;
	// 是否启用数据处理
	private boolean enabled;

	// 统计目标单位
	private List<DataProcessUnit> targetUnits = new ArrayList<>();
	// 最近处理的数据日期
	private Map<String, Long> lastProcessedDayMap = new HashMap<>();

	/**
	 * 更新某单位最近一次处理数据日期
	 * 
	 * @param unitId
	 * @param lastDayTime
	 */
	public void updateLastProcessedDay(String unitId, long lastDayTime) {
		lastProcessedDayMap.put(unitId, lastDayTime);
	}

	/**
	 * 获取某单位最近一次处理数据日期
	 * 
	 * @param unitId
	 * @return
	 */
	public long getLastProcessedDay(String unitId) {
		return lastProcessedDayMap.get(unitId);
	}

	/**
	 * 是否为该事件处理的目标单位
	 * 
	 * @param unitId
	 * @return
	 */
	public boolean isTargetUnit(String unitId) {
		for (DataProcessUnit unit : targetUnits) {
			if (unit.getId().equals(unitId)) {
				return true;
			}
		}
		return false;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getEventType() {
		return eventType;
	}

	public void setEventType(int eventType) {
		this.eventType = eventType;
	}

	public int getTargetType() {
		return targetType;
	}

	public void setTargetType(int targetType) {
		this.targetType = targetType;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public int getProcessBefore() {
		return processBefore;
	}

	public void setProcessBefore(int processBefore) {
		this.processBefore = processBefore;
	}

	public int getProcessBeforeType() {
		return processBeforeType;
	}

	public void setProcessBeforeType(int processBeforeType) {
		this.processBeforeType = processBeforeType;
	}

	public boolean isRealTimeEnabled() {
		return realTimeEnabled;
	}

	public void setRealTimeEnabled(boolean realTimeEnabled) {
		this.realTimeEnabled = realTimeEnabled;
	}

	public int getRealTimeInterval() {
		return realTimeInterval;
	}

	public void setRealTimeInterval(int realTimeInterval) {
		this.realTimeInterval = realTimeInterval;
	}

	public int getSqlSpeed() {
		return sqlSpeed;
	}

	public void setSqlSpeed(int sqlSpeed) {
		this.sqlSpeed = sqlSpeed;
	}

	public boolean isSeparateProcessThread() {
		return separateProcessThread;
	}

	public void setSeparateProcessThread(boolean separateProcessThread) {
		this.separateProcessThread = separateProcessThread;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public List<DataProcessUnit> getTargetUnits() {
		return targetUnits;
	}

	public void setTargetUnits(List<DataProcessUnit> targetUnits) {
		this.targetUnits = targetUnits;
	}

	public Date getProcessStartDate() {
		return processStartDate;
	}

	public void setProcessStartDate(Date processStartDate) {
		this.processStartDate = TimeUtil.toDayTime(processStartDate);
	}
}
