package com.paladin.qos.dynamic.mapper.migration;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

public interface DataMigrateMapper {

	public List<Map<String, Object>> selectData(@Param("sql") String sql);

	public int insertData(@Param("sql") String sql);

	public int updateData(@Param("sql") String sql);

}
