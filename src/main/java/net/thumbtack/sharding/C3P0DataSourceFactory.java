package net.thumbtack.sharding;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSourceFactory;

import java.util.Properties;

public class C3P0DataSourceFactory extends UnpooledDataSourceFactory {

	public C3P0DataSourceFactory() {
		// disable verbose internal logging of pool
		Properties p = new Properties(System.getProperties());
		System.setProperties(p);

		this.dataSource = new ComboPooledDataSource();
	}

}