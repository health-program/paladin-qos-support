package com.paladin.qos.controller.migration;

import java.text.ParseException;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.framework.web.response.CommonResponse;
import com.paladin.qos.migration.DataMigratorContainer;
import com.paladin.qos.migration.increment.IncrementDataMigrator;

@Controller
@RequestMapping("/qos/migrate")
public class MigrateController {

	@Autowired
	private DataMigratorContainer container;

	@GetMapping("/execute/test")
	@ResponseBody
	public Object execute(String id) {
		IncrementDataMigrator migrator = container.getIncrementDataMigrator(id);
		
		try {
			Date updateTime = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm").parse("2018-01-01 00:00:00");
			migrator.migrateData(updateTime, null, 500);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return CommonResponse.getSuccessResponse();
	}

}
