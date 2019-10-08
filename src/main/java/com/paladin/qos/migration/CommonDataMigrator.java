package com.paladin.qos.migration;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.utils.time.DateFormatUtil;
import com.paladin.qos.dynamic.mapper.migration.DataMigrateMapper;

public class CommonDataMigrator implements DataMigrator {

	public final static String ORIGIN_DATA_SOURCE_TYPE_MYSQL = "mysql";
	public final static String ORIGIN_DATA_SOURCE_TYPE_SQLSERVER = "sqlserver";
	public final static String ORIGIN_DATA_SOURCE_TYPE_ORACLE = "oracle";

	private static Logger logger = LoggerFactory.getLogger(CommonDataMigrator.class);

	private SqlSessionContainer sqlSessionContainer;

	/**
	 * 原始数据源
	 */
	private String originDataSource;

	/**
	 * 原始数据库表名
	 */
	private String originTableName;

	/**
	 * 原始数据源类型
	 */
	private String originDataSourceType;

	/**
	 * 目标数据源
	 */
	private String targetDataSource;

	/**
	 * 目标数据库表名
	 */
	private String targetTableName;

	/**
	 * 获取数据限制条目
	 * 
	 * @return
	 */
	private int selectDataLimit = 100;

	/**
	 * 最大迁移数据量
	 * 
	 * @return
	 */
	private int migrateMaximum = 500;

	/**
	 * 更新时间字段
	 */
	private String updateTimeField;

	/**
	 * 主键字段（可多个）
	 */
	private Set<String> primaryKeyFields;

	/**
	 * 获取数据SQL
	 * 
	 * @param updateTime
	 * @param limit
	 * @return
	 */
	public String createSelectDataSql(Date updateTime, int limit) {
		if (ORIGIN_DATA_SOURCE_TYPE_MYSQL.equals(originDataSourceType)) {
			return createMysqlSelectDataSql(updateTime, limit);
		} else if (ORIGIN_DATA_SOURCE_TYPE_SQLSERVER.equals(originDataSourceType)) {
			return createSqlServerSelectDataSql(updateTime, limit);
		} else if (ORIGIN_DATA_SOURCE_TYPE_ORACLE.equals(originDataSourceType)) {
			return createOracleSelectDataSql(updateTime, limit);
		}

		throw new RuntimeException("不存在类型[" + originDataSourceType + "]数据库");
	}

	private String createMysqlSelectDataSql(Date updateTime, int limit) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ").append(originTableName).append(" WHERE ").append(updateTimeField).append(">='")
				.append(DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm").format(updateTime)).append("' ORDER BY ").append(updateTimeField)
				.append(" ASC LIMIT ").append(limit);
		return sb.toString();
	}

	private String createSqlServerSelectDataSql(Date updateTime, int limit) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT TOP ").append(limit).append(" * FROM ").append(originTableName).append(" WHERE ").append(updateTimeField).append(">='")
				.append(DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm").format(updateTime)).append("' ORDER BY ").append(updateTimeField)
				.append(" ASC");
		return sb.toString();
	}

	private String createOracleSelectDataSql(Date updateTime, int limit) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ").append(originTableName).append(" WHERE ").append("rownum <=").append(limit).append(" AND ").append(updateTimeField)
				.append(">=to_date('").append(DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm").format(updateTime))
				.append("','yyyy-mm-dd hh24:mi:ss')  ORDER BY ").append(updateTimeField).append(" ASC");
		return sb.toString();
	}

	public MigrateResult migrateData(Date updateTime) {
		MigrateResult result = new MigrateResult(updateTime);
		do {
			try {
				updateTime = result.getMigrateEndTime();
				boolean readMax = migrateData(updateTime, result, selectDataLimit);
				int total = result.getMigrateNum();

				if (!readMax || !result.isSuccess() || (migrateMaximum > 0 && total >= migrateMaximum)) {
					break;
				}

			} catch (Exception e) {
				logger.error("更新或插入数据失败！数据源：[" + originDataSource + "到" + targetDataSource + "]，表名：[" + originTableName + "到" + targetTableName + "]，增量时间点："
						+ updateTime, e);
				result.setSuccess(false);
				break;
			}

		} while (true);

		return result;
	}

	protected boolean migrateData(Date updateTime, MigrateResult result, int selectDataLimit) {

		List<Map<String, Object>> datas = getData(updateTime, selectDataLimit);

		if (datas != null) {
			int size = datas.size();
			if (size >= selectDataLimit) {
				Date start = (Date) datas.get(0).get(updateTimeField);
				Date end = (Date) datas.get(size - 1).get(updateTimeField);
				// 如果查出数据都是一个时间点，则需要扩大搜索范围以保证不进入死循环
				if (start.getTime() == end.getTime()) {
					return migrateData(updateTime, result, selectDataLimit * 2);
				}
			}

			for (Map<String, Object> data : datas) {
				Map<String, Object> needData = processData(data);

				boolean success = insertOrUpdateData(needData);

				if (success) {
					Date time = (Date) needData.get(updateTimeField);
					result.setMigrateEndTime(time);
					result.setMigrateNum(result.getMigrateNum() + 1);
				} else {
					logger.error("更新或插入数据失败！数据源：[" + originDataSource + "到" + targetDataSource + "]，表名：[" + originTableName + "到" + targetTableName + "]，增量时间点："
							+ updateTime);
					result.setSuccess(false);
					break;
				}
			}

			return size >= selectDataLimit;

		}

		return false;
	}

	protected List<Map<String, Object>> getData(Date updateTime, int limit) {
		sqlSessionContainer.setCurrentDataSource(originDataSource);
		String sql = createSelectDataSql(updateTime, limit);
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class).selectData(sql);
	}

	/**
	 * 重写该方法以达到处理数据，默认为不处理
	 * 
	 * @param data
	 * @return
	 */
	public Map<String, Object> processData(Map<String, Object> data) {
		return data;
	}

	public boolean insertOrUpdateData(Map<String, Object> data) {

		StringBuilder keys = new StringBuilder();
		StringBuilder values = new StringBuilder();

		StringBuilder updateSql = new StringBuilder();
		updateSql.append("UPDATE ").append(targetTableName).append(" SET ");

		for (Entry<String, Object> entry : data.entrySet()) {

			String key = entry.getKey();
			Object value = entry.getValue();

			String valueStr = convertValue2SqlString(value);

			keys.append(key).append(',');
			values.append(valueStr).append(',');

			if (!primaryKeyFields.contains(key)) {
				updateSql.append(key).append("=").append(valueStr).append(',');
			}
		}

		keys.deleteCharAt(keys.length() - 1);
		values.deleteCharAt(values.length() - 1);

		updateSql.deleteCharAt(updateSql.length() - 1);
		updateSql.append(" WHERE ");

		boolean first = true;

		for (String primaryKeyField : primaryKeyFields) {

			Object value = data.get(primaryKeyField);
			if (value == null) {
				return false;
			}

			String valueStr = convertValue2SqlString(value);

			if (first) {
				updateSql.append(primaryKeyField).append('=').append(valueStr);
				first = false;
			} else {
				updateSql.append(" AND ").append(primaryKeyField).append('=').append(valueStr);
			}
		}

		sqlSessionContainer.setCurrentDataSource(targetDataSource);
		DataMigrateMapper sqlMapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);

		String update = updateSql.toString();

		if (sqlMapper.updateData(update) > 0) {
			return true;
		} else {
			String insert = "INSERT INTO " + targetTableName + " (" + keys.toString() + ") VALUES (" + values.toString() + ")";
			return sqlMapper.insertData(insert) > 0;
		}
	}

	private String convertValue2SqlString(Object value) {
		if (value == null) {
			return "null";
		} else if (value instanceof Date) {
			return "'" + DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm").format((Date) value) + "'";
		} else if (value instanceof Boolean) {
			return ((Boolean) value) ? "1" : "0";
		} else {
			return "'" + value.toString() + "'";
		}
	}

	public int getSelectDataLimit() {
		return selectDataLimit;
	}

	public void setSelectDataLimit(int selectDataLimit) {
		this.selectDataLimit = selectDataLimit;
	}

	public int getMigrateMaximum() {
		return migrateMaximum;
	}

	public void setMigrateMaximum(int migrateMaximum) {
		this.migrateMaximum = migrateMaximum;
	}

	public String getUpdateTimeField() {
		return updateTimeField;
	}

	public void setUpdateTimeField(String updateTimeField) {
		this.updateTimeField = updateTimeField;
	}

	public SqlSessionContainer getSqlSessionContainer() {
		return sqlSessionContainer;
	}

	public void setSqlSessionContainer(SqlSessionContainer sqlSessionContainer) {
		this.sqlSessionContainer = sqlSessionContainer;
	}

	public String getOriginDataSource() {
		return originDataSource;
	}

	public void setOriginDataSource(String originDataSource) {
		this.originDataSource = originDataSource;
	}

	public String getOriginTableName() {
		return originTableName;
	}

	public void setOriginTableName(String originTableName) {
		this.originTableName = originTableName;
	}

	public String getTargetDataSource() {
		return targetDataSource;
	}

	public void setTargetDataSource(String targetDataSource) {
		this.targetDataSource = targetDataSource;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public void setPrimaryKeyFields(Set<String> primaryKeyFields) {
		this.primaryKeyFields = primaryKeyFields;
	}

	public Set<String> getPrimaryKeyFields() {
		return primaryKeyFields;
	}

	public String getOriginDataSourceType() {
		return originDataSourceType;
	}

	public void setOriginDataSourceType(String originDataSourceType) {
		this.originDataSourceType = originDataSourceType;
	}

}
