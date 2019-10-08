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
import com.paladin.qos.migration.DataMigrator;
import com.paladin.qos.migration.DataMigratorContainer;

@Controller
@RequestMapping("/qos/migrate")
public class MigrateController {

	@Autowired
	private DataMigratorContainer container;

	@GetMapping("/test")
	@ResponseBody
	public Object processDataSchedule() {

		DataMigrator migrator = container.getDataMigratorList().get(0);
		try {
			Date updateTime = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm").parse("2019-01-01 00:00:00");
			migrator.migrateData(updateTime);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return CommonResponse.getSuccessResponse();
	}

}
