package com.paladin.qos.controller.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.paladin.framework.web.response.CommonResponse;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskManager;

@Controller
@RequestMapping("/qos/data/task")
public class DataTaskController {
	@Autowired
	private DataTaskManager taskManager;

	@GetMapping("/info")
	@ResponseBody
	public Object getTaskStatus() {
		Map<String, List<DataTaskVO>> result = new HashMap<>();

		result.put("dawn", convertVO(taskManager.getDawnTasks()));
		result.put("night", convertVO(taskManager.getNightTasks()));
		result.put("realtime", convertVO(taskManager.getRealTimeTasks()));

		return CommonResponse.getSuccessResponse(result);
	}

	@GetMapping("/execute")
	@ResponseBody
	public Object executeTask(String id) {
		DataTask task = taskManager.getTask(id);
		task.run();
		return CommonResponse.getSuccessResponse();
	}

	private List<DataTaskVO> convertVO(List<DataTask> tasks) {
		if (tasks != null) {
			List<DataTaskVO> vos = new ArrayList<>(tasks.size());
			for (DataTask task : tasks) {
				vos.add(new DataTaskVO(task));
			}
			return vos;
		}
		return null;
	}
}
