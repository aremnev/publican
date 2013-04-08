package net.thumbtack.sharding.tests;


import net.thumbtack.sharding.AbstractDaoTest;
import net.thumbtack.sharding.ShardedDao;
import net.thumbtack.sharding.UserDao;
import net.thumbtack.sharding.query.QueryFactory;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class ShardedDaoTest extends AbstractDaoTest {

	@Test
	public void smoke() throws IOException {
		Map<Integer, SqlSessionFactory> sessionFactoryMap = ShardedDao.buildSessionFactoryMap(new Properties());
		QueryFactory queryFactory = new QueryFactory(sessionFactoryMap);

		UserDao userDao = new UserDao(queryFactory);
	}

}
