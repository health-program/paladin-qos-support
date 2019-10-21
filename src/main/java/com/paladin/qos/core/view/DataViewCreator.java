package com.paladin.qos.core.view;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.core.DataViewCreateMapper;
import com.paladin.qos.model.data.DataView;

public class DataViewCreator {

	private static Logger logger = LoggerFactory.getLogger(DataViewCreator.class);

	private DataView dataView;

	protected SqlSessionContainer sqlSessionContainer;

	/** ID 唯一标识 */
	protected String id;

	/** 原始数据源 */
	protected String originDataSource;

	/** 目标数据源 */
	protected String targetDataSource;

	/** 查询视图SQL */
	protected String selectSql;

	public DataViewCreator(DataView dataView, SqlSessionContainer sqlSessionContainer) {
		setDataView(dataView);
		this.sqlSessionContainer = sqlSessionContainer;
	}

	public DataView getDataView() {
		return dataView;
	}

	public void setDataView(DataView dataView) {
		this.dataView = dataView;
		this.id = dataView.getId();
		this.originDataSource = dataView.getOriginDataSource();
		this.targetDataSource = dataView.getTargetDataSource();
		this.selectSql = dataView.getSelectSql();
	}

	public boolean updateView() {
		try {
			String viewJson = getDataJson();
			return insertOrUpdateData(viewJson);
		} catch (Exception e) {
			logger.error("更新视图[" + id + "]异常", e);
			return false;
		}
	}

	private ObjectMapper objectMapper = new ObjectMapper();

	private String getDataJson() {
		sqlSessionContainer.setCurrentDataSource(originDataSource);
		DataViewCreateMapper mapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataViewCreateMapper.class);

		List<Map<String, Object>> viewData = mapper.selectView(selectSql);

		try {
			return objectMapper.writeValueAsString(viewData);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean insertOrUpdateData(String viewJson) {
		sqlSessionContainer.setCurrentDataSource(targetDataSource);
		DataViewCreateMapper mapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataViewCreateMapper.class);
		if (mapper.updateViewData(id, viewJson) <= 0) {
			return mapper.insertViewData(id, viewJson) > 0;
		}
		return true;
	}

	public String getId() {
		return id;
	}

}
