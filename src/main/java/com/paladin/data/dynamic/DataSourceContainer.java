package com.paladin.data.dynamic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.paladin.framework.core.VersionContainer;

@Component
@ConditionalOnProperty(prefix = "paladin", value = "dynamic-datasource-enabled", havingValue = "true", matchIfMissing = false)
public class DataSourceContainer implements VersionContainer {

	@Resource
	private DynamicDataSourceProperties  properties;
	
	private static Map<String, DataSourceFacade> dsMap = new HashMap<>();

	public void initialize() {
		List<DataSourceConfig> sources =  properties.getSource();
		if(sources != null) {
			for(DataSourceConfig source : sources) {
				dsMap.put(source.getName(), new DataSourceFacade(source));
			}
		}
	}

	public static DataSource getRealDataSource(String name) {
		DataSourceFacade facade = dsMap.get(name);
		return facade == null ? null : facade.getRealDataSource();
	}

	public static DataSourceFacade getDataSource(String name) {
		return dsMap.get(name);
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
