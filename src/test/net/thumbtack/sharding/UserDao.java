package net.thumbtack.sharding;

import net.thumbtack.sharding.query.QueryFactory;

import java.util.List;


public class UserDao extends ShardedDao<User> {

	public UserDao(QueryFactory queryFactory) {
		super(queryFactory);
	}

	@Override
	public Object select(long id) {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public List selectAll() {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public Object insert(Object entity) {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public boolean update(Object entity) {
		return false;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public boolean delete(Object entity) {
		return false;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public boolean delete(long id) {
		return false;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public Void deleteAll() {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
