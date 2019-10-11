package com.paladin.qos.controller.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.paladin.framework.web.response.CommonResponse;
import com.paladin.qos.migration.DataMigratorContainer;
import com.paladin.qos.migration.increment.IncrementDataMigrator;
import com.paladin.qos.migration.increment.IncrementDataMigrator.MigrateResult;

@Controller
@RequestMapping("/qos/migrate")
public class MigrateController {

	@Autowired
	private DataMigratorContainer container;

	@GetMapping("/execute/test")
	@ResponseBody
	public Object execute(String id) {
		IncrementDataMigrator migrator = container.getIncrementDataMigrator(id);
		MigrateResult result = migrator.migrateData(migrator.getScheduleStartTime(), null, 500);
		return CommonResponse.getSuccessResponse(result);
	}

}
