package com.paladin.qos.dynamic.mapper.migration;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

public interface DataMigrateMapper {

	public List<Map<String, Object>> selectData(@Param("sql") String sql);

	public int insertData(@Param("tableName") String tableName, @Param("dataMap") Map<String, Object> dataMap);

	public int updateData(@Param("tableName") String tableName, @Param("dataMap") Map<String, Object> dataMap,
			@Param("primaryMap") Map<String, Object> primaryMap);

	public List<Date> getMaxUpdateTime(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField);

}
