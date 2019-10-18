package com.paladin.qos.dynamic.mapper.core;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

public interface DataMigrateMapper {

	public int insertData(@Param("tableName") String tableName, @Param("dataMap") Map<String, Object> dataMap);

	public int updateData(@Param("tableName") String tableName, @Param("dataMap") Map<String, Object> dataMap,
			@Param("primaryMap") Map<String, Object> primaryMap);

	public List<Date> getMaxUpdateTime(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField);

	public List<String> getMaxUpdateTimeByYear(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField);

	public List<Map<String, Object>> selectDataForMySql(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField,
			@Param("selectColumns") String selectColumns, @Param("startTime") String startTime, @Param("endTime") String endTime, @Param("limit") int limit);

	public List<Map<String, Object>> selectDataForSqlServer(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField,
			@Param("selectColumns") String selectColumns, @Param("startTime") String startTime, @Param("endTime") String endTime, @Param("limit") int limit);

	public List<Map<String, Object>> selectDataForOracle(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField,
			@Param("selectColumns") String selectColumns, @Param("startTime") String startTime, @Param("endTime") String endTime, @Param("limit") int limit);

	public List<Map<String, Object>> selectDataForOracleToMillisecond(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField,
			@Param("selectColumns") String selectColumns, @Param("startTime") String startTime, @Param("endTime") String endTime, @Param("limit") int limit);

	public List<Map<String, Object>> selectDataByYear(@Param("tableName") String tableName, @Param("updateTimeField") String updateTimeField,
			@Param("selectColumns") String selectColumns, @Param("years") List<String> years);

}
