package com.paladin.qos.controller.analysis;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.paladin.framework.web.response.CommonResponse;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.AnalysisService;
import com.paladin.qos.service.data.DataEventService;
import com.paladin.qos.util.TimeUtil;

@Controller
@RequestMapping("/qos/analysis")
public class AnalysisController {

	@Autowired
	private AnalysisService analysisService;

	@Autowired
	private DataEventService dataEventService;

	@GetMapping("/process/index")
	public Object processIndex() {
		return "/qos/analysis/process_index";
	}

	@GetMapping("/processed/index")
	public Object dataIndex() {
		return "/qos/analysis/processed_index";
	}

	@PostMapping("/data/process")
	@ResponseBody
	public Object processData(AnalysisRequest request) {
		List<String> eventIds = request.getEventIds();
		List<String> unitIds = request.getUnitIds();
		Date startDate = request.getStartTime();
		Date endDate = request.getEndTime();

		List<DataUnit> units = null;
		if (unitIds != null && unitIds.size() > 0) {
			units = new ArrayList<>(unitIds.size());
			for (String unitId : unitIds) {
				DataUnit unit = DataConstantContainer.getUnit(unitId);
				if (unit != null) {
					units.add(unit);
				}
			}
		}

		List<DataEvent> events = null;
		if (eventIds != null && eventIds.size() > 0) {
			events = new ArrayList<>(eventIds.size());
			for (String eventId : eventIds) {
				DataEvent event = DataConstantContainer.getEvent(eventId);
				if (event != null) {
					events.add(event);
				}
			}
		}

		if (events == null || events.size() == 0) {
			return CommonResponse.getFailResponse("请传入事件");
		}

		if (analysisService.processDataByThread(startDate, endDate, events, units)) {
			return CommonResponse.getSuccessResponse();
		} else {
			return CommonResponse.getFailResponse("有正在处理中的数据");
		}
	}

	@GetMapping("/data/process/status")
	@ResponseBody
	public Object getProcessDataStatus() {
		return CommonResponse.getSuccessResponse(analysisService.getProcessDataStatus());
	}

	@PostMapping("/data/validate")
	@ResponseBody
	public Object validateProcessedData(AnalysisRequest request) {
		Date startDate = request.getStartTime();
		Date endDate = request.getEndTime();
		return CommonResponse.getSuccessResponse(analysisService.validateProcessedData(startDate, endDate));
	}

	@PostMapping("/data/test")
	@ResponseBody
	public Object testProcessor() {
		return CommonResponse.getSuccessResponse(analysisService.testProcessors());
	}

	@PostMapping("/data/get/day/instalments")
	@ResponseBody
	public Object getDataOfDayByInstalments(AnalysisRequest request) {
		List<String> eventIds = request.getEventIds();
		Date startDate = request.getStartTime();
		Date endDate = request.getEndTime();
		boolean byUnit = request.getByUnit() == 1;

		if (eventIds != null && eventIds.size() > 0) {
			Map<String, Object> map = new HashMap<>();
			for (String eventId : eventIds) {
				DataEvent event = DataConstantContainer.getEvent(eventId);
				if (event != null) {
					int unitType = getUnitType(event);
					Object item = byUnit ? analysisService.getDataSetOfDay(eventId, unitType, startDate, endDate)
							: analysisService.getAnalysisResultByDay(eventId, unitType, startDate, endDate);
					if (item != null) {
						map.put(eventId, item);
					}
				}
			}
			return CommonResponse.getSuccessResponse(map);
		} else {
			String eventId = request.getEventId();
			return CommonResponse.getSuccessResponse(analysisService.getDataSetOfDay(eventId, startDate, endDate));
		}
	}

	@PostMapping("/data/get/month/instalments")
	@ResponseBody
	public Object getDataOfMonthByInstalments(AnalysisRequest request) {
		List<String> eventIds = request.getEventIds();
		Date startDate = request.getStartTime();
		Date endDate = request.getEndTime();
		boolean byUnit = request.getByUnit() == 1;

		if (eventIds != null && eventIds.size() > 0) {
			Map<String, Object> map = new HashMap<>();
			for (String eventId : eventIds) {
				DataEvent event = DataConstantContainer.getEvent(eventId);
				if (event != null) {
					int unitType = getUnitType(event);
					Object item = byUnit ? analysisService.getDataSetOfMonth(eventId, unitType, startDate, endDate)
							: analysisService.getAnalysisResultByMonth(eventId, unitType, startDate, endDate);
					if (item != null) {
						map.put(eventId, item);
					}
				}
			}
			return CommonResponse.getSuccessResponse(map);
		} else {
			String eventId = request.getEventId();
			return CommonResponse.getSuccessResponse(analysisService.getDataSetOfMonth(eventId, startDate, endDate));
		}
	}

	@PostMapping("/data/get/year/instalments")
	@ResponseBody
	public Object getDataOfYearByInstalments(AnalysisRequest request) {
		Date startDate = request.getStartTime();
		Date endDate = request.getEndTime();
		int startYear = TimeUtil.getYear(startDate);
		int endYear = TimeUtil.getYear(endDate);
		boolean byUnit = request.getByUnit() == 1;

		List<String> eventIds = request.getEventIds();
		if (eventIds != null && eventIds.size() > 0) {
			Map<String, Object> map = new HashMap<>();
			for (String eventId : eventIds) {
				DataEvent event = DataConstantContainer.getEvent(eventId);
				if (event != null) {
					int unitType = getUnitType(event);
					Object item = byUnit ? analysisService.getDataSetOfYear(eventId, unitType, startYear, endYear)
							: analysisService.getAnalysisResultByYear(eventId, unitType, startYear, endYear);
					if (item != null) {
						map.put(eventId, item);
					}
				}
			}
			return CommonResponse.getSuccessResponse(map);
		} else {
			String eventId = request.getEventId();
			return CommonResponse.getSuccessResponse(analysisService.getDataSetOfYear(eventId, startYear, endYear));
		}
	}

	@RequestMapping(value = "/data/get/unit", method = { RequestMethod.GET, RequestMethod.POST })
	@ResponseBody
	public Object getProcessedDataByUnit(AnalysisRequest request) {
		Date startDate = request.getStartTime();
		Date endDate = request.getEndTime();

		List<String> eventIds = request.getEventIds();
		if (eventIds != null && eventIds.size() > 0) {
			Map<String, Object> map = new HashMap<>();
			for (String eventId : eventIds) {
				DataEvent event = DataConstantContainer.getEvent(eventId);
				if (event != null) {
					int eventType = event.getEventType();
					int unitType = getUnitType(event);
					if (DataEvent.EVENT_TYPE_COUNT == eventType) {
						Object item = analysisService.countTotalNumByUnit(eventId, unitType, startDate, endDate);
						if (item != null) {
							map.put(eventId, item);
						}
					} else if (DataEvent.EVENT_TYPE_RATE == eventType) {
						Object item = analysisService.getAnalysisResultByUnit(eventId, unitType, startDate, endDate);
						if (item != null) {
							map.put(eventId, item);
						}
					}
				}
			}
			return CommonResponse.getSuccessResponse(map);
		} else {
			String eventId = request.getEventId();
			DataEvent event = DataConstantContainer.getEvent(eventId);
			if (event != null) {
				int eventType = event.getEventType();
				int unitType = getUnitType(event);
				if (DataEvent.EVENT_TYPE_COUNT == eventType) {
					return CommonResponse.getSuccessResponse(analysisService.countTotalNumByUnit(eventId, unitType, startDate, endDate));
				} else if (DataEvent.EVENT_TYPE_RATE == eventType) {
					return CommonResponse.getSuccessResponse(analysisService.getAnalysisResultByUnit(eventId, unitType, startDate, endDate));
				}
			}
		}
		return CommonResponse.getErrorResponse();
	}

	@RequestMapping(value = "/data/get/total", method = { RequestMethod.GET, RequestMethod.POST })
	@ResponseBody
	public Object getProcessedDataTotal(AnalysisRequest request) {
		Date startDate = request.getStartTime();
		Date endDate = request.getEndTime();

		List<String> eventIds = request.getEventIds();

		if (eventIds != null && eventIds.size() > 0) {
			Map<String, Long> map = new HashMap<>();
			for (String eventId : eventIds) {
				DataEvent event = DataConstantContainer.getEvent(eventId);
				if (event != null) {
					long count = analysisService.getTotalNumOfEvent(eventId, startDate, endDate);
					map.put(eventId, count);
				}
			}
			return CommonResponse.getSuccessResponse(map);
		} else {
			String eventId = request.getEventId();
			return CommonResponse.getSuccessResponse(analysisService.getTotalNumOfEvent(eventId, startDate, endDate));
		}
	}

	private int getUnitType(DataEvent event) {
		int targetType = event.getTargetType();
		if (targetType == DataEvent.TARGET_TYPE_COMMUNITY)
			return DataUnit.TYPE_COMMUNITY;
		if (targetType == DataEvent.TARGET_TYPE_HOSPITAL)
			return DataUnit.TYPE_HOSPITAL;
		return 0;
	}

	@GetMapping("/constant/event")
	@ResponseBody
	public Object getEvent() {
		return CommonResponse.getSuccessResponse(dataEventService.findAll());
	}

}
