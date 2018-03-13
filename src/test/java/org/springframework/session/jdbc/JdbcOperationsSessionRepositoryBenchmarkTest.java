package org.springframework.session.jdbc;

import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class JdbcOperationsSessionRepositoryBenchmarkTest extends AbstractJdbcOperationsSessionRepositoryTest {

	@Test
	public void testUpstream() {
		long initial = getDatabaseSize();
		createTables();
		for (int i = 0; i < 1_000; i++) {
			JdbcOperationsSessionRepository.JdbcSession session = repository.createSession();
			session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar" + i);
			repository.save(session);
		}
		System.out.println("Upstream  " + (getDatabaseSize() - initial));
	}

	@Test
	public void testAlternate() {
		long initial = getDatabaseSize();
		createAltTables();
		for (int i = 0; i < 1_000; i++) {
			JdbcOperationsSessionRepositoryAlternate.JdbcSession session = altRepository.createSession();
			session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar" + i);
			altRepository.save(session);
		}
		System.out.println("Alternate " + (getDatabaseSize() - initial));
	}

	private long getDatabaseSize() {
		template.execute("VACUUM FULL");
		return template.queryForObject("select pg_database_size('sessiontest')", Long.class);
	}

}
