package com.paladin.data.dynamic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.paladin.framework.core.VersionContainer;

@Component
@ConditionalOnProperty(prefix = "paladin", value = "dynamic-datasource-enabled", havingValue = "true", matchIfMissing = false)
public class DataSourceContainer implements VersionContainer {

	private static String staticLocalSourceName;
	private static DataSource staticLocalDataSource;

	@Resource
	private DynamicDataSourceProperties properties;

	@Autowired(required = false)
	private DataSource localDataSource;

	private static Map<String, DataSourceFacade> dsMap = new HashMap<>();

	public void initialize() {
		staticLocalSourceName = properties.getLocalSourceName();
		staticLocalDataSource = localDataSource;

		List<DataSourceConfig> sources = properties.getSource();
		if (sources != null) {
			for (DataSourceConfig source : sources) {
				dsMap.put(source.getName(), new DataSourceFacade(source));
			}
		}
	}

	public static DataSource getRealDataSource(String name) {
		if (staticLocalDataSource != null && staticLocalSourceName.equals(name)) {
			return staticLocalDataSource;
		}

		DataSourceFacade facade = dsMap.get(name);
		return facade == null ? null : facade.getRealDataSource();
	}

	@Override
	public boolean versionChangedHandle(long version) {
		initialize();
		return false;
	}

	@Override
	public String getId() {
		return "data_source_container";
	}
}
