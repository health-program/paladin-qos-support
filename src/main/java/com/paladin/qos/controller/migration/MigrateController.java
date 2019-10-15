package com.paladin.qos.controller.migration;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.paladin.framework.web.response.CommonResponse;
import com.paladin.qos.core.migration.DataMigratorContainer;
import com.paladin.qos.core.migration.IncrementDataMigrator;
import com.paladin.qos.core.migration.IncrementDataMigrator.MigrateResult;

@Controller
@RequestMapping("/qos/migrate")
public class MigrateController {

	@Autowired
	private DataMigratorContainer container;

	@GetMapping("/index")
	@ResponseBody
	public Object index() {
		return "/qos/migration/index";
	}

	@RequestMapping("/execute")
	@ResponseBody
	public Object execute(MigrateRequest request) {
		String id = request.getId();
		Date startTime = request.getStartTime();
		Date endTime = request.getEndTime();
		int num = request.getMigrateNum();

		IncrementDataMigrator migrator = container.getIncrementDataMigrator(id);

		if (startTime == null) {
			startTime = migrator.getCurrentUpdateTime();
		}

		MigrateResult result = migrator.migrateData(startTime, endTime, num);
		return CommonResponse.getSuccessResponse(result);
	}

}
