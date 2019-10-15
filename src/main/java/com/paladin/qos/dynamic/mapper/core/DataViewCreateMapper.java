package com.paladin.qos.dynamic.mapper.core;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

public interface DataViewCreateMapper {

	public List<Map<String, Object>> selectView(@Param("sql") String selectSql);

	public int insertViewData(@Param("id")String id, @Param("data") String data);

	public int updateViewData(@Param("id")String id, @Param("data") String data);

}
