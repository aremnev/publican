package net.thumbtack.sharding.query;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public interface QueryEngine extends Executor {
	Collection<Integer> shardsKeys();

	SqlSession openSession(ExecutorType type);

	SqlSession openSession(long id, ExecutorType type);

	SqlSessionFactory sessionFactory(long id);

	int shardsCount();
}
