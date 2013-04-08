package net.thumbtack.sharding.tests;


import net.thumbtack.sharding.*;
import net.thumbtack.sharding.query.SqlQueryEngine;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class ShardedDaoTest extends AbstractDaoTest {

	@Test
	public void smoke() throws IOException {
		Map<Integer, SqlSessionFactory> sessionFactoryMap = ShardedDao.buildSessionFactoryMap(new Properties());
		SqlQueryEngine sqlQueryEngine = new SqlQueryEngine(sessionFactoryMap);

		UserDao userDao = new UserDao(sqlQueryEngine);
	}

	public void config() {
		// storage wide configuration here:
		//   replication and rebalancing logic
		Sharding sharding = new Sharding(new Properties());

		SqlShard sqlShard = SQlQueryEngine.newShard(new Properties());
		MemShard memShard = MemQueryEngine.newShard(new Properties());

		// per-shard configuration here
		sharding.addShard("1", sqlShard);
		sharding.addShard("2", memShard);
		sharding.addShard("3", sqlShard);

		//   mapping key -> shard
		sharding.addKeyMapper(new ModuloKeyMapper(sharding));

		UserDao userDao = new UserDao(sharding);
	}

}
