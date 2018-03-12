package com.sopovs.moradanen.spring.session;

import static com.sopovs.moradanen.spring.session.JdbcOperationsSessionRepositoryAlternate.DEFAULT_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.PostgreSQLContainer;

import com.sopovs.moradanen.spring.session.JdbcOperationsSessionRepositoryAlternate.JdbcSession;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class JdbcOperationsSessionRepositoryAlternateTest {

	@Rule
	public PostgreSQLContainer postgres = new PostgreSQLContainer();

	private JdbcOperationsSessionRepositoryAlternate repository;
	private HikariDataSource ds;
	private JdbcTemplate template;

	private JdbcSession session;

	@Before
	public void setup() {
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setMaximumPoolSize(1);
		hikariConfig.setJdbcUrl(postgres.getJdbcUrl());
		hikariConfig.setUsername(postgres.getUsername());
		hikariConfig.setPassword(postgres.getPassword());

		ds = new HikariDataSource(hikariConfig);
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		template = new JdbcTemplate(ds);
		repository = new JdbcOperationsSessionRepositoryAlternate(template, tm);

		template.execute("DROP TABLE IF EXISTS " + DEFAULT_TABLE_NAME + "_ATTRIBUTES");
		template.execute("DROP TABLE IF EXISTS " + DEFAULT_TABLE_NAME);

		ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
		populator.addScript(new ClassPathResource("/com/sopovs/moradanen/spring/session/schema-postgresql.sql"));
		populator.setContinueOnError(true);
		DatabasePopulatorUtils.execute(populator, ds);

		session = repository.createSession();
	}

	@After
	public void tearDown() {
		ds.close();
	}

	@Test
	public void testCreate() {
		repository.save(repository.createSession());
	}

	@Test
	public void testCreateAndRead() {
		JdbcSession session = repository.createSession();
		repository.save(session);
		JdbcSession got = repository.findById(session.getId());

		assertSessionEquals(session, got);
	}

	@Test
	public void testDoubleSaveEmpty() {
		repository.save(session);
		repository.save(session);
	}

	@Test
	public void testSaveWithPrincipalName() {
		session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		repository.save(session);
	}

	@Test
	public void testDoubleSaveWithPrincipalName() {
		testSaveWithPrincipalName();
		repository.save(session);
	}

	@Test
	public void testSaveChangedSessionId() {
		repository.save(session);
		session.changeSessionId();
		repository.save(session);
	}

	@Test
	public void testFindById() {
		repository.save(session);
		assertSessionEquals(repository.findById(session.getId()));
	}

	@Test
	public void testFindByIdWithPrincipalName() {
		testSaveWithPrincipalName();
		JdbcSession got = repository.findById(session.getId());
		assertSessionEquals(got);
		assertEquals("foobar", got.getPrincipalName());
		assertEquals(1, got.getAttributeNames().size());
	}

	@Test
	public void testDeleteByIdNotSaved() {
		repository.deleteById(session.getId());
	}

	@Test
	public void testDeleteById() {
		repository.save(session);
		repository.deleteById(session.getId());
		assertNull(repository.findById(session.getId()));
	}

	@Test
	public void testDeleteByIdWIthAttribute() {
		session.setAttribute("foo", "bar");
		repository.save(session);
		repository.deleteById(session.getId());
		assertNull(repository.findById(session.getId()));
	}

	@Test
	public void testRemoveAttribute() {
		session.setAttribute("foo", "bar");
		repository.save(session);
		assertSessionEquals(repository.findById(session.getId()));

		session.removeAttribute("foo");
		repository.save(session);
		JdbcSession got = repository.findById(session.getId());
		assertSessionEquals(got);
		assertEquals(0, got.getAttributeNames().size());
	}

	@Test
	public void testFindByNewlyChangedId() {
		session.changeSessionId();
		repository.save(session);
		assertSessionEquals(repository.findById(session.getId()));
	}

	@Test
	public void testFindByChangedId() {
		repository.save(session);
		String prevId = session.getId();
		session.changeSessionId();
		repository.save(session);

		assertNull(repository.findById(prevId));
		assertSessionEquals(repository.findById(session.getId()));
	}

	@Test
	public void testFindByIndexNameAndIndexValue() {
		testSaveWithPrincipalName();
		Map<String, JdbcSession> result = repository.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		assertEquals(1, result.size());
		assertSessionEquals(session, result.get(session.getId()));
	}

	@Test
	public void testFind2ByIndexNameAndIndexValue() {
		testSaveWithPrincipalName();

		JdbcSession session2 = repository.createSession();
		session2.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		repository.save(session2);

		Map<String, JdbcSession> result = repository.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		assertEquals(2, result.size());
		assertSessionEquals(result.get(session.getId()));
		assertSessionEquals(session2, result.get(session2.getId()));
	}

	@Test
	public void testEqualEpirations() {
		JdbcSession session2 = repository.createSession();
		Instant now = Instant.now();
		session.setLastAccessedTime(now);
		session2.setLastAccessedTime(now);
		repository.save(session);
		repository.save(session2);

		assertEquals(repository.findById(session.getId()).getExpiryTime(),
				repository.findById(session2.getId()).getExpiryTime());
	}

	@Test
	public void testCleanUpExpiredSessions() {
		session.setLastAccessedTime(Instant.now().minus(Duration.ofDays(100)));
		repository.save(session);
		repository.cleanUpExpiredSessions();
		assertEquals(0, (int) template.queryForObject("SELECT COUNT(0) FROM " + DEFAULT_TABLE_NAME, Integer.class));
	}

	@Test
	public void testFindRemovesExpired() {
		session.setLastAccessedTime(Instant.now().minus(Duration.ofDays(100)));
		repository.save(session);
		assertNull(repository.findById(session.getId()));
	}

	private void assertSessionEquals(JdbcSession actual) {
		assertSessionEquals(session, actual);
	}

	private void assertSessionEquals(JdbcSession expected, JdbcSession actual) {
		assertEquals(expected.getId(), actual.getId());
		assertEquals(expected.getLastAccessedTime().toEpochMilli(), actual.getLastAccessedTime().toEpochMilli());
		assertEquals(expected.getMaxInactiveInterval(), actual.getMaxInactiveInterval());
		assertEquals(expected.getExpiryTime().toEpochMilli(), actual.getExpiryTime().toEpochMilli());
		assertEquals(expected.getCreationTime().toEpochMilli(), actual.getCreationTime().toEpochMilli());
		assertEquals(expected.getAttributeNames(), actual.getAttributeNames());
		assertEquals(expected.getPrincipalName(), actual.getPrincipalName());
		for (String attribute : expected.getAttributeNames()) {
			assertEquals((Object) expected.getAttribute(attribute), (Object) actual.getAttribute(attribute));
		}
	}

}
