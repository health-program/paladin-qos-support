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

	private static Map<String, DataEvent> eventMap;
	private static Map<String, DataUnit> unitMap;

	private static List<DataEvent> events;
	private static List<DataUnit> units;
	private static List<DataUnit> hospitals;
	private static List<DataUnit> communities;

	public boolean initialize() {
		List<DataEvent> events = dataEventService.findAll();
		List<DataUnit> units = dataUnitService.findAll();

		Map<String, DataEvent> eventMap = new HashMap<>();
		Map<String, DataUnit> unitMap = new HashMap<>();

		List<KeyValue> eventKeyValues = new ArrayList<>();

		for (DataEvent event : events) {
			String id = event.getId();
			String name = event.getName();

			eventKeyValues.add(new KeyValue(id, name));
			eventMap.put(id, event);
		}

		for (DataUnit unit : units) {
			String id = unit.getId();
			units.add(unit);
			unitMap.put(id, unit);
		}

		units.sort(new Comparator<DataUnit>() {
			@Override
			public int compare(DataUnit o1, DataUnit o2) {
				int i1 = o1.getOrderNum();
				int i2 = o2.getOrderNum();
				return i1 > i2 ? 1 : -1;
			}
		});

		List<DataUnit> hospitals = new ArrayList<>();
		List<DataUnit> communities = new ArrayList<>();

		List<KeyValue> unitKeyValues = new ArrayList<>();
		List<KeyValue> hospitalKeyValues = new ArrayList<>();
		List<KeyValue> communityKeyValues = new ArrayList<>();

		for (DataUnit unit : units) {
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

		return true;
	}

	public static void updateData() {
		VersionContainerManager.versionChanged(container_id);
	}

	public static List<DataEvent> getEventList() {
		return events;
	}

	public static List<DataEvent> getEventListByDataSource(String dataSouce) {
		List<DataEvent> events = new ArrayList<>();
		for (DataEvent event : DataConstantContainer.events) {
			if (event.getDataSource().equals(dataSouce)) {
				events.add(event);
			}
		}
		return events;
	}

	public static List<DataUnit> getUnitList() {
		return units;
	}

	public static List<DataUnit> getHospitalList() {
		return hospitals;
	}

	public static List<DataUnit> getCommunityList() {
		return communities;
	}

	public static DataUnit getUnit(String id) {
		return unitMap.get(id);
	}

	public static DataEvent getEvent(String id) {
		return eventMap.get(id);
	}

	public static String getUnitName(String id) {
		DataUnit unit = unitMap.get(id);
		return unit == null ? "未知单位" : unit.getName();
	}

	public static String getEventName(String id) {
		DataEvent event = eventMap.get(id);
		return event == null ? "未知统计事件" : event.getName();
	}

}
