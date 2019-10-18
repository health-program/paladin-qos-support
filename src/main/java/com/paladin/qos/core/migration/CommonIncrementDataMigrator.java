package com.paladin.qos.core.migration;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.dynamic.mapper.core.DataMigrateMapper;
import com.paladin.qos.model.migration.DataMigration;

import oracle.sql.TIMESTAMP;

/**
 * 增量数据迁移实现类
 * 
 * @author TontoZhou
 * @since 2019年10月9日
 */
public class CommonIncrementDataMigrator implements IncrementDataMigrator {

	protected static final int MAX_SELECT_DATA_LIMIT = 10000;

	private static Logger logger = LoggerFactory.getLogger(CommonIncrementDataMigrator.class);

	protected SqlSessionContainer sqlSessionContainer;

	/**
	 * ID 唯一标识
	 */
	protected String id;

	/**
	 * 原始数据源
	 */
	protected String originDataSource;

	/**
	 * 原始数据库表名
	 */
	protected String originTableName;

	/**
	 * 原始数据源类型
	 */
	protected String originDataSourceType;

	/**
	 * 目标数据源
	 */
	protected String targetDataSource;

	/**
	 * 目标数据库表名
	 */
	protected String targetTableName;

	/**
	 * 更新时间字段
	 */
	protected String updateTimeField;

	/**
	 * 主键字段（可多个）
	 */
	protected Set<String> primaryKeyFields;

	/**
	 * 查询列
	 */
	protected String selectColumns;

	/**
	 * 缺省开始时间
	 */
	protected Date defaultStartDate;

	/**
	 * 是否精确到毫秒
	 */
	protected boolean millisecondEnabled = false;

	/**
	 * 数据迁移描述
	 */
	protected DataMigration dataMigration;

	public CommonIncrementDataMigrator(DataMigration dataMigration, SqlSessionContainer sqlSessionContainer) {
		this.setDataMigration(dataMigration);
		this.sqlSessionContainer = sqlSessionContainer;
	}

	@Override
	public String getId() {
		return id;
	}

	public void setDataMigration(DataMigration dataMigration) {
		this.id = dataMigration.getId();
		this.originDataSource = dataMigration.getOriginDataSource();
		this.originDataSourceType = dataMigration.getOriginDataSourceType();
		this.originTableName = dataMigration.getOriginTableName();
		this.targetDataSource = dataMigration.getTargetDataSource();
		this.targetTableName = dataMigration.getTargetTableName();
		this.updateTimeField = dataMigration.getUpdateTimeField();
		this.defaultStartDate = dataMigration.getDefaultStartDate();
		this.millisecondEnabled = dataMigration.getMillisecondEnabled() == 1;
		this.selectColumns = dataMigration.getSelectColumns();

		if (this.selectColumns == null || this.selectColumns.length() == 0) {
			this.selectColumns = "*";
		}

		String primaryKeyField = dataMigration.getPrimaryKeyField();
		String[] keyFields = primaryKeyField.split(",");

		this.primaryKeyFields = new HashSet<>();
		for (String keyField : keyFields) {
			primaryKeyFields.add(keyField);
		}

		this.dataMigration = dataMigration;
	}

	public DataMigration getDataMigration() {
		return dataMigration;
	}

	public MigrateResult migrateData(Date updateStartTime, Date updateEndTime, int migrateNum) {
		MigrateResult result = new MigrateResult(updateStartTime);

		updateStartTime = result.getMigrateEndTime();
		if (updateEndTime != null && updateStartTime.getTime() > updateEndTime.getTime()) {
			return result;
		}

		try {
			migrateData(updateStartTime, updateEndTime, result, migrateNum);
		} catch (Exception e) {
			logger.error("迁移数据失败！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime, e);
			result.setSuccess(false);
		}

		return result;
	}

	protected void migrateData(Date updateStartTime, Date updateEndTime, MigrateResult result, int selectDataLimit) {
		if (selectDataLimit > MAX_SELECT_DATA_LIMIT) {
			logger.error("超过最大查询限制数据！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime);
			result.setSuccess(false);
			return;
		}

		List<Map<String, Object>> datas = getData(updateStartTime, updateEndTime, selectDataLimit);

		if (datas != null) {
			int size = datas.size();
			if (size >= selectDataLimit) {
				Date start = getDataUpdateTime(datas.get(0));
				Date end = getDataUpdateTime(datas.get(size - 1));
				// 如果查出数据都是一个时间点，则需要扩大搜索范围以保证不进入死循环
				if (start.getTime() == end.getTime()) {
					migrateData(updateStartTime, updateEndTime, result, selectDataLimit * 2);
					return;
				}
			}

			for (Map<String, Object> data : datas) {
				Map<String, Object> needData = processData(data);
				boolean success = insertOrUpdateData(needData);
				if (success) {
					Date time = getDataUpdateTime(needData);
					result.setMigrateEndTime(time);
					result.setMigrateNum(result.getMigrateNum() + 1);
				} else {
					logger.error("更新或插入数据失败！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime);
					result.setSuccess(false);
					return;
				}
			}
		}
	}

	/**
	 * 获取原始数据
	 * 
	 * @param updateStartTime
	 * @param updateEndTime
	 * @param limit
	 * @return
	 */
	protected List<Map<String, Object>> getData(Date updateStartTime, Date updateEndTime, int limit) {
		sqlSessionContainer.setCurrentDataSource(originDataSource);
		DataMigrateMapper mapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);
		SimpleDateFormat format = DateFormatUtil.getThreadSafeFormat(millisecondEnabled ? "yyyy-MM-dd HH:ss:mm.sss" : "yyyy-MM-dd HH:ss:mm");

		String startTime = format.format(updateStartTime);
		String endTime = updateEndTime != null ? format.format(updateEndTime) : null;

		if (DataMigration.ORIGIN_DATA_SOURCE_TYPE_MYSQL.equals(originDataSourceType)) {
			return mapper.selectDataForMySql(originTableName, updateTimeField, selectColumns, startTime, endTime, limit);
		} else if (DataMigration.ORIGIN_DATA_SOURCE_TYPE_SQLSERVER.equals(originDataSourceType)) {
			return mapper.selectDataForSqlServer(originTableName, updateTimeField, selectColumns, startTime, endTime, limit);
		} else if (DataMigration.ORIGIN_DATA_SOURCE_TYPE_ORACLE.equals(originDataSourceType)) {
			return millisecondEnabled ? mapper.selectDataForOracleToMillisecond(originTableName, updateTimeField, selectColumns, startTime, endTime, limit)
					: mapper.selectDataForOracle(originTableName, updateTimeField, selectColumns, startTime, endTime, limit);
		}

		throw new RuntimeException("不存在类型[" + originDataSourceType + "]数据库");
	}

	protected Date getDataUpdateTime(Map<String, Object> data) {
		Object date = data.get(updateTimeField);
		if (date != null) {
			if (date instanceof TIMESTAMP) {
				try {
					return ((TIMESTAMP) date).dateValue();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else {
				return (Date) date;
			}
		}
		return null;
	}

	/**
	 * 重写该方法以达到处理数据，默认为不处理
	 * 
	 * @param data
	 * @return
	 */
	protected Map<String, Object> processData(Map<String, Object> data) {
		for (Entry<String, Object> entry : data.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof Boolean) {
				data.put(entry.getKey(), ((Boolean) value) ? "1" : "0");
			}
		}
		return data;
	}

	/**
	 * 插入或更新数据到目标数据源
	 * 
	 * @param dataMap
	 * @return
	 */
	protected boolean insertOrUpdateData(Map<String, Object> dataMap) {
		sqlSessionContainer.setCurrentDataSource(targetDataSource);
		DataMigrateMapper sqlMapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);

		Map<String, Object> primaryMap = new HashMap<>();

		for (String primaryKey : primaryKeyFields) {
			Object value = dataMap.get(primaryKey);
			if (value == null) {
				logger.error("主键不能为空！数据迁移ID：" + id);
				return false;
			}
			primaryMap.put(primaryKey, value);
			dataMap.remove(primaryKey);
		}

		if (sqlMapper.updateData(targetTableName, dataMap, primaryMap) > 0) {
			return true;
		} else {
			dataMap.putAll(primaryMap);
			return sqlMapper.insertData(targetTableName, dataMap) > 0;
		}
	}

	@Override
	public Date getCurrentUpdateTime() {
		sqlSessionContainer.setCurrentDataSource(targetDataSource);
		DataMigrateMapper sqlMapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);
		List<Date> current = sqlMapper.getMaxUpdateTime(targetTableName, updateTimeField);
		return (current == null || current.size() == 0) ? defaultStartDate : current.get(0);
	}

}
