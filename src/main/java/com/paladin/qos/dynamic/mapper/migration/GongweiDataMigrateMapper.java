package com.paladin.qos.dynamic.mapper.migration;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

public interface GongweiDataMigrateMapper {

	public List<Map<String, Object>> selectData200001(@Param("years") List<String> years);

}
