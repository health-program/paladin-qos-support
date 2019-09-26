package com.paladin.qos.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.paladin.common.core.container.ConstantsContainer;
import com.paladin.common.core.container.ConstantsContainer.KeyValue;
import com.paladin.framework.core.VersionContainer;
import com.paladin.framework.core.VersionContainerManager;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.data.DataEventService;
import com.paladin.qos.service.data.DataUnitService;

@Component
public class DataConstantContainer implements VersionContainer {

	private final static String container_id = "data_constant_container";

	private final static int DEFAULT_REAL_TIME_INTERVAL = 5;

	@Override
	public String getId() {
		return container_id;
	}

	private final static String TYPE_EVENT = "data-event-type";
	private final static String TYPE_UNIT = "data-unit-type"; // 所有单位
	private final static String TYPE_HOSPITAL = "data-hospital-type"; // 医院
	private final static String TYPE_COMMUNITY = "data-community-type"; // 社区

	@Autowired
	private ConstantsContainer constantsContainer;
	@Autowired
	private DataEventService dataEventService;
	@Autowired
	private DataUnitService dataUnitService;
	@Autowired
	private DataProcessManager dataProcessManager;

	private static Map<String, Event> eventMap;
	private static Map<String, Unit> unitMap;

	private static List<Event> events;
	private static List<Unit> units;
	private static List<Unit> hospitals;
	private static List<Unit> communities;

	public boolean initialize() {

		List<DataEvent> dataEvents = dataEventService.findAll();
		List<DataUnit> dataUnits = dataUnitService.findAll();

		List<Event> events = new ArrayList<>();
		List<Unit> units = new ArrayList<>();

		Map<String, Event> eventMap = new HashMap<>();
		Map<String, Unit> unitMap = new HashMap<>();

		List<KeyValue> eventKeyValues = new ArrayList<>();

		for (DataEvent dataEvent : dataEvents) {
			String id = dataEvent.getId();
			String name = dataEvent.getName();
			Integer enabled = dataEvent.getEnabled();
			Integer realTimeEnabled = dataEvent.getRealTimeEnabled();
			Integer realTimeInterval = dataEvent.getRealTimeInterval();

			eventKeyValues.add(new KeyValue(id, name));
			Event event = new Event();
			event.setId(id);
			event.setName(name);
			event.setEnabled(enabled != null && enabled.intValue() == 1);
			event.setEventType(dataEvent.getEventType());
			event.setDataSource(dataEvent.getDataSource());
			event.setTargetType(dataEvent.getTargetType());
			event.setRealTimeEnabled(realTimeEnabled != null && realTimeEnabled.intValue() == 1);
			event.setRealTimeInterval(realTimeInterval == null ? DEFAULT_REAL_TIME_INTERVAL : realTimeInterval);
			event.setProcessBefore(dataEvent.getProcessBefore());

			events.add(event);
			eventMap.put(id, event);
		}

		for (DataUnit dataUnit : dataUnits) {
			String id = dataUnit.getId();
			String name = dataUnit.getName();
			Integer type = dataUnit.getType();
			Integer orderNum = dataUnit.getOrderNum();

			Unit unit = new Unit();
			unit.setId(id);
			unit.setName(name);
			unit.setType(type);
			unit.setSource(dataUnit);
			unit.setOrderNum(orderNum == null ? 999999 : orderNum.intValue());

			units.add(unit);
			unitMap.put(id, unit);
		}

		units.sort(new Comparator<Unit>() {
			@Override
			public int compare(Unit o1, Unit o2) {
				int i1 = o1.getOrderNum();
				int i2 = o2.getOrderNum();
				return i1 > i2 ? 1 : -1;
			}
		});

		List<Unit> hospitals = new ArrayList<>();
		List<Unit> communities = new ArrayList<>();

		List<KeyValue> unitKeyValues = new ArrayList<>();
		List<KeyValue> hospitalKeyValues = new ArrayList<>();
		List<KeyValue> communityKeyValues = new ArrayList<>();

		for (Unit unit : units) {
			String id = unit.getId();
			String name = unit.getName();
			int type = unit.getType();

			unitKeyValues.add(new KeyValue(id, name));
			if (type == DataUnit.TYPE_HOSPITAL) {
				hospitals.add(unit);
				hospitalKeyValues.add(new KeyValue(id, name));
			} else if (type == DataUnit.TYPE_COMMUNITY) {
				communities.add(unit);
				communityKeyValues.add(new KeyValue(id, name));
			}

		}

		constantsContainer.putConstant(TYPE_EVENT, eventKeyValues);
		constantsContainer.putConstant(TYPE_UNIT, unitKeyValues);
		constantsContainer.putConstant(TYPE_HOSPITAL, hospitalKeyValues);
		constantsContainer.putConstant(TYPE_COMMUNITY, communityKeyValues);

		DataConstantContainer.events = Collections.unmodifiableList(events);
		DataConstantContainer.units = Collections.unmodifiableList(units);

		DataConstantContainer.eventMap = Collections.unmodifiableMap(eventMap);
		DataConstantContainer.unitMap = Collections.unmodifiableMap(unitMap);

		DataConstantContainer.hospitals = Collections.unmodifiableList(hospitals);
		DataConstantContainer.communities = Collections.unmodifiableList(communities);

		dataProcessManager.readLastProcessedDay(null);		
		return true;
	}

	@Override
	public boolean versionChangedHandle(long version) {
		initialize();
		return false;
	}

	public static void updateData() {
		VersionContainerManager.versionChanged(container_id);
	}

	public static List<Event> getEventList() {
		return events;
	}

	public static List<Event> getEventListByDataSource(String dataSouce) {
		List<Event> events = new ArrayList<>();
		for (Event event : DataConstantContainer.events) {
			if (event.getDataSource().equals(dataSouce)) {
				events.add(event);
			}
		}
		return events;
	}

	public static List<Unit> getUnitList() {
		return units;
	}

	public static List<Unit> getHospitalList() {
		return hospitals;
	}

	public static List<Unit> getCommunityList() {
		return communities;
	}

	public static List<Unit> getUnitListByType(int targetType) {
		if (targetType == DataEvent.TARGET_TYPE_ALL) {
			return DataConstantContainer.getUnitList();
		} else if (targetType == DataEvent.TARGET_TYPE_HOSPITAL) {
			return DataConstantContainer.getHospitalList();
		} else if (targetType == DataEvent.TARGET_TYPE_COMMUNITY) {
			return DataConstantContainer.getCommunityList();
		}
		return null;
	}

	public static Unit getUnit(String id) {
		return unitMap.get(id);
	}

	public static Event getEvent(String id) {
		return eventMap.get(id);
	}

	public static String getUnitName(String id) {
		Unit unit = unitMap.get(id);
		return unit == null ? "未知单位" : unit.getName();
	}

	public static String getEventName(String id) {
		Event event = eventMap.get(id);
		return event == null ? "未知统计事件" : event.getName();
	}

	public static class Event {

		private String id;
		private String name;
		private int eventType;
		private int targetType;
		private String dataSource;
		private int processBefore;
		private boolean realTimeEnabled;
		private int realTimeInterval;
		private boolean enabled;

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

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
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
	}

	public static class Unit {

		@JsonIgnore
		private DataUnit source;

		private String id;
		private String name;
		private int type;
		private int orderNum;

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

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public DataUnit getSource() {
			return source;
		}

		public void setSource(DataUnit source) {
			this.source = source;
		}

		public int getOrderNum() {
			return orderNum;
		}

		public void setOrderNum(int orderNum) {
			this.orderNum = orderNum;
		}
	}

	public int order() {
		// 需要在常量容器后执行
		return 10;
	};

}
