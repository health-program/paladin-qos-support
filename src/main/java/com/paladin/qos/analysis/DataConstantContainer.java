package com.paladin.qos.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

	private final static long DEFAULT_REAL_TIME_INTERVAL = 5 * 60 * 1000;

	@Override
	public String getId() {
		return container_id;
	}

	public int order() {
		// 需要在常量容器后执行
		return 10;
	};

	@Override
	public boolean versionChangedHandle(long version) {
		initialize();
		return false;
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

	private static Map<String, DataProcessEvent> eventMap;
	private static Map<String, DataProcessUnit> unitMap;

	private static List<DataProcessEvent> events;
	private static List<DataProcessUnit> units;
	private static List<DataProcessUnit> hospitals;
	private static List<DataProcessUnit> communities;

	public boolean initialize() {
		List<DataEvent> dataEvents = dataEventService.findAll();
		List<DataUnit> dataUnits = dataUnitService.findAll();

		List<DataProcessEvent> events = new ArrayList<>();
		List<DataProcessUnit> units = new ArrayList<>();

		Map<String, DataProcessEvent> eventMap = new HashMap<>();
		Map<String, DataProcessUnit> unitMap = new HashMap<>();

		List<KeyValue> eventKeyValues = new ArrayList<>();

		for (DataEvent dataEvent : dataEvents) {
			String id = dataEvent.getId();
			String name = dataEvent.getName();
			Integer enabled = dataEvent.getEnabled();
			Integer realTimeEnabled = dataEvent.getRealTimeEnabled();
			Integer realTimeInterval = dataEvent.getRealTimeInterval();
			Integer separateProcessThread = dataEvent.getSeparateProcessThread();

			DataProcessEvent event = new DataProcessEvent();
			event.setId(id);
			event.setName(name);
			event.setEnabled(enabled != null && enabled.intValue() == 1);
			event.setEventType(dataEvent.getEventType());
			event.setDataSource(dataEvent.getDataSource());
			event.setTargetType(dataEvent.getTargetType());
			event.setRealTimeEnabled(realTimeEnabled != null && realTimeEnabled.intValue() == 1);
			event.setRealTimeInterval(realTimeInterval == null ? DEFAULT_REAL_TIME_INTERVAL : realTimeInterval * 60 * 1000);
			event.setProcessBefore(dataEvent.getProcessBefore());
			event.setProcessBeforeType(dataEvent.getProcessBeforeType());
			event.setSqlSpeed(dataEvent.getSqlSpeed());
			event.setSeparateProcessThread(separateProcessThread != null && separateProcessThread.intValue() == 1);

			events.add(event);

			eventKeyValues.add(new KeyValue(id, name));
			eventMap.put(id, event);
		}

		for (DataUnit dataUnit : dataUnits) {
			String id = dataUnit.getId();
			String name = dataUnit.getName();
			Integer type = dataUnit.getType();
			Integer orderNum = dataUnit.getOrderNum();

			DataProcessUnit unit = new DataProcessUnit();
			unit.setId(id);
			unit.setName(name);
			unit.setType(type);
			unit.setSource(dataUnit);
			unit.setOrderNum(orderNum == null ? 999999 : orderNum.intValue());

			units.add(unit);
			unitMap.put(id, unit);
		}

		units.sort(new Comparator<DataProcessUnit>() {
			@Override
			public int compare(DataProcessUnit o1, DataProcessUnit o2) {
				int i1 = o1.getOrderNum();
				int i2 = o2.getOrderNum();
				return i1 > i2 ? 1 : -1;
			}
		});

		List<DataProcessUnit> hospitals = new ArrayList<>();
		List<DataProcessUnit> communities = new ArrayList<>();

		List<KeyValue> unitKeyValues = new ArrayList<>();
		List<KeyValue> hospitalKeyValues = new ArrayList<>();
		List<KeyValue> communityKeyValues = new ArrayList<>();

		for (DataProcessUnit unit : units) {
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

		for (DataProcessEvent event : events) {
			int targetType = event.getTargetType();
			if (targetType == DataProcessEvent.TARGET_TYPE_ALL) {
				event.setTargetUnits(DataConstantContainer.units);
			} else if (targetType == DataProcessEvent.TARGET_TYPE_HOSPITAL) {
				event.setTargetUnits(DataConstantContainer.hospitals);
			} else if (targetType == DataProcessEvent.TARGET_TYPE_COMMUNITY) {
				event.setTargetUnits(DataConstantContainer.communities);
			}
		}

		dataProcessManager.readLastProcessedDay(null);
		return true;
	}

	public static void updateData() {
		VersionContainerManager.versionChanged(container_id);
	}

	public static List<DataProcessEvent> getEventList() {
		return events;
	}

	public static List<DataProcessEvent> getEventListByDataSource(String dataSouce) {
		List<DataProcessEvent> events = new ArrayList<>();
		for (DataProcessEvent event : DataConstantContainer.events) {
			if (event.getDataSource().equals(dataSouce)) {
				events.add(event);
			}
		}
		return events;
	}

	public static List<DataProcessUnit> getUnitList() {
		return units;
	}

	public static List<DataProcessUnit> getHospitalList() {
		return hospitals;
	}

	public static List<DataProcessUnit> getCommunityList() {
		return communities;
	}

	public static DataProcessUnit getUnit(String id) {
		return unitMap.get(id);
	}

	public static DataProcessEvent getEvent(String id) {
		return eventMap.get(id);
	}

	public static String getUnitName(String id) {
		DataProcessUnit unit = unitMap.get(id);
		return unit == null ? "未知单位" : unit.getName();
	}

	public static String getEventName(String id) {
		DataProcessEvent event = eventMap.get(id);
		return event == null ? "未知统计事件" : event.getName();
	}

}
