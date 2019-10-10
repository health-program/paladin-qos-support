package com.paladin.qos.migration.increment;

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
import com.paladin.qos.dynamic.mapper.migration.DataMigrateMapper;
import com.paladin.qos.model.migration.DataMigration;

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
	 * 获取数据限制条目
	 * 
	 * @return
	 */
	protected int selectDataLimit;

	/**
	 * 更新时间字段
	 */
	protected String updateTimeField;

	/**
	 * 主键字段（可多个）
	 */
	protected Set<String> primaryKeyFields;

	/**
	 * 缺省默认开始更新时间
	 */
	protected Date defaultStartDate;

	/**
	 * 当前更新时间
	 */
	protected Date scheduleStartTime;

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
		this.selectDataLimit = dataMigration.getSelectDataLimit();
		this.updateTimeField = dataMigration.getUpdateTimeField();
		this.defaultStartDate = dataMigration.getDefaultStartDate();

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
		do {
			try {
				updateStartTime = result.getMigrateEndTime();

				if (updateEndTime != null && updateStartTime.getTime() > updateEndTime.getTime()) {
					break;
				}

				boolean readMax = migrateData(updateStartTime, updateEndTime, result, selectDataLimit);
				int total = result.getMigrateNum();

				if (!readMax || !result.isSuccess() || (migrateNum > 0 && total >= migrateNum)) {
					break;
				}

			} catch (Exception e) {
				logger.error("迁移数据失败！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime, e);
				result.setSuccess(false);
				break;
			}
		} while (true);

		return result;
	}

	protected boolean migrateData(Date updateStartTime, Date updateEndTime, MigrateResult result, int selectDataLimit) {

		if (selectDataLimit > MAX_SELECT_DATA_LIMIT) {
			logger.error("超过最大查询限制数据！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime);
			result.setSuccess(false);
			return false;
		}

		List<Map<String, Object>> datas = getData(updateStartTime, updateEndTime, selectDataLimit);

		if (datas != null) {
			int size = datas.size();
			if (size >= selectDataLimit) {
				Date start = (Date) datas.get(0).get(updateTimeField);
				Date end = (Date) datas.get(size - 1).get(updateTimeField);
				// 如果查出数据都是一个时间点，则需要扩大搜索范围以保证不进入死循环
				if (start.getTime() == end.getTime()) {
					return migrateData(updateStartTime, updateEndTime, result, selectDataLimit * 2);
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
					logger.error("更新或插入数据失败！数据迁移ID：" + id + "，更新开始时间点：" + updateStartTime);
					result.setSuccess(false);
					return false;
				}
			}

			return size >= selectDataLimit;
		}

		return false;
	}

	protected List<Map<String, Object>> getData(Date updateStartTime, Date updateEndTime, int limit) {
		sqlSessionContainer.setCurrentDataSource(originDataSource);
		DataMigrateMapper mapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);
		SimpleDateFormat format = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd HH:ss:mm");

		String startTime = format.format(updateStartTime);
		String endTime = updateEndTime != null ? format.format(updateEndTime) : null;

		if (DataMigration.ORIGIN_DATA_SOURCE_TYPE_MYSQL.equals(originDataSourceType)) {
			return mapper.selectDataForMySql(originTableName, updateTimeField, startTime, endTime, limit);
		} else if (DataMigration.ORIGIN_DATA_SOURCE_TYPE_SQLSERVER.equals(originDataSourceType)) {
			return mapper.selectDataForSqlServer(originTableName, updateTimeField, startTime, endTime, limit);
		} else if (DataMigration.ORIGIN_DATA_SOURCE_TYPE_ORACLE.equals(originDataSourceType)) {
			return mapper.selectDataForOracle(originTableName, updateTimeField, startTime, endTime, limit);
		}

		throw new RuntimeException("不存在类型[" + originDataSourceType + "]数据库");
	}

	/**
	 * 重写该方法以达到处理数据，默认为不处理
	 * 
	 * @param data
	 * @return
	 */
	public Map<String, Object> processData(Map<String, Object> data) {
		for (Entry<String, Object> entry : data.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof Boolean) {
				data.put(entry.getKey(), ((Boolean) value) ? "1" : "0");
			}
		}
		return data;
	}

	public boolean insertOrUpdateData(Map<String, Object> dataMap) {

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
	public Date getScheduleStartTime() {
		if (scheduleStartTime == null) {
			sqlSessionContainer.setCurrentDataSource(targetDataSource);
			DataMigrateMapper sqlMapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataMigrateMapper.class);
			List<Date> current = sqlMapper.getMaxUpdateTime(targetTableName, updateTimeField);
			scheduleStartTime = (current == null || current.size() == 0) ? defaultStartDate : current.get(0);
		}
		return scheduleStartTime;
	}

	@Override
	public void setScheduleStartTime(Date scheduleStartTime) {
		this.scheduleStartTime = scheduleStartTime;
	}

}
