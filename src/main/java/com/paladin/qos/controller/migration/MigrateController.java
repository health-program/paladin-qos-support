package com.paladin.qos.controller.migration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

	public static Date DEFAULT_START_TIME;

	static {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			DEFAULT_START_TIME = format.parse("2019-01-01");
		} catch (ParseException e) {
		}
	}

	@Autowired
	private DataMigratorContainer container;

	@GetMapping("/execute/test")
	@ResponseBody
	public Object execute(String id) {
		IncrementDataMigrator migrator = container.getIncrementDataMigrator(id);
		MigrateResult result = migrator.migrateData(DEFAULT_START_TIME, null, 500);
		return CommonResponse.getSuccessResponse(result);
	}

}
