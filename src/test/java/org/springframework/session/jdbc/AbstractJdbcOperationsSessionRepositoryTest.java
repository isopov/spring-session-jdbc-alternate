package org.springframework.session.jdbc;

import org.junit.After;
import org.junit.Before;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.session.jdbc.JdbcOperationsSessionRepository;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class AbstractJdbcOperationsSessionRepositoryTest {
	protected HikariDataSource ds;
	protected JdbcTemplate template;

	protected JdbcOperationsSessionRepositoryAlternate altRepository;
	protected JdbcOperationsSessionRepository repository;

	@Before
	public void setup() {
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setMaximumPoolSize(1);
		hikariConfig.setJdbcUrl("jdbc:postgresql://localhost/sessiontest");
		hikariConfig.setUsername("sessiontest");
		hikariConfig.setPassword("sessiontest");

		ds = new HikariDataSource(hikariConfig);
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		template = new JdbcTemplate(ds);
		altRepository = new JdbcOperationsSessionRepositoryAlternate(template, tm);
		repository = new JdbcOperationsSessionRepository(template, tm);
		dropTables();
	}

	protected void createAltTables() {
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
		populator.addScript(new ClassPathResource("/org/springframework/session/jdbc/schema-alt-postgresql.sql"));
		populator.setContinueOnError(true);
		DatabasePopulatorUtils.execute(populator, ds);
	}

	protected void createTables() {
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
		populator.addScript(new ClassPathResource("/org/springframework/session/jdbc/schema-postgresql.sql"));
		populator.setContinueOnError(true);
		DatabasePopulatorUtils.execute(populator, ds);

		template.execute("DROP INDEX IF EXISTS SPRING_SESSION_ATTRIBUTES_IX1");

	}

	private void dropTables() {
		template.execute(
				"DROP TABLE IF EXISTS " + JdbcOperationsSessionRepositoryAlternate.DEFAULT_TABLE_NAME + "_ATTRIBUTES");
		template.execute("DROP TABLE IF EXISTS " + JdbcOperationsSessionRepositoryAlternate.DEFAULT_TABLE_NAME);

		template.execute("DROP TABLE IF EXISTS " + JdbcOperationsSessionRepository.DEFAULT_TABLE_NAME + "_ATTRIBUTES");
		template.execute("DROP TABLE IF EXISTS " + JdbcOperationsSessionRepository.DEFAULT_TABLE_NAME);
	}

	@After
	public void tearDown() {
		dropTables();
		ds.close();
	}
}
