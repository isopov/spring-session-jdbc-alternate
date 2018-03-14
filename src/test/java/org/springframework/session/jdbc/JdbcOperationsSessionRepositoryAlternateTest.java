package org.springframework.session.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;
import static org.springframework.session.jdbc.JdbcOperationsSessionRepositoryAlternate.DEFAULT_TABLE_NAME;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.session.jdbc.JdbcOperationsSessionRepositoryAlternate.JdbcSession;

public class JdbcOperationsSessionRepositoryAlternateTest extends AbstractJdbcOperationsSessionRepositoryTest {
	private JdbcSession session;

	@Before
	public void setup() {
		super.setup();
		createAltTables();
		session = altRepository.createSession();
	}

	@Test
	public void testCreate() {
		altRepository.save(altRepository.createSession());
	}

	@Test
	public void testCreateAndRead() {
		JdbcSession session = altRepository.createSession();
		altRepository.save(session);
		JdbcSession got = altRepository.findById(session.getId());

		assertSessionEquals(session, got);
	}

	@Test
	public void testDoubleSaveEmpty() {
		altRepository.save(session);
		altRepository.save(session);
	}

	@Test
	public void testSaveWithPrincipalName() {
		session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		altRepository.save(session);
	}

	@Test
	public void testDoubleSaveWithPrincipalName() {
		testSaveWithPrincipalName();
		altRepository.save(session);
	}

	@Test
	public void testTwoAttributes() {
		session.setAttribute("foo", "bar");
		session.setAttribute("bar", "foo");
		altRepository.save(session);
		assertSessionEquals(altRepository.findById(session.getId()));
	}

	@Test
	public void testSaveChangedSessionId() {
		altRepository.save(session);
		session.changeSessionId();
		altRepository.save(session);
	}

	@Test
	public void testFindById() {
		altRepository.save(session);
		assertSessionEquals(altRepository.findById(session.getId()));
	}

	@Test
	public void testFindByIdWithPrincipalName() {
		testSaveWithPrincipalName();
		JdbcSession got = altRepository.findById(session.getId());
		assertSessionEquals(got);
		assertEquals("foobar", got.getPrincipalName());
		assertEquals(1, got.getAttributeNames().size());
	}

	@Test
	public void testDeleteByIdNotSaved() {
		altRepository.deleteById(session.getId());
	}

	@Test
	public void testDeleteById() {
		altRepository.save(session);
		altRepository.deleteById(session.getId());
		assertNull(altRepository.findById(session.getId()));
	}

	@Test
	public void testDeleteByIdWIthAttribute() {
		session.setAttribute("foo", "bar");
		altRepository.save(session);
		altRepository.deleteById(session.getId());
		assertNull(altRepository.findById(session.getId()));
	}

	@Test
	public void testRemoveAttribute() {
		session.setAttribute("foo", "bar");
		altRepository.save(session);
		assertSessionEquals(altRepository.findById(session.getId()));

		session.removeAttribute("foo");
		altRepository.save(session);
		JdbcSession got = altRepository.findById(session.getId());
		assertSessionEquals(got);
		assertEquals(0, got.getAttributeNames().size());
	}

	@Test
	public void testFindByNewlyChangedId() {
		session.changeSessionId();
		altRepository.save(session);
		assertSessionEquals(altRepository.findById(session.getId()));
	}

	@Test
	public void testFindByChangedId() {
		altRepository.save(session);
		String prevId = session.getId();
		session.changeSessionId();
		altRepository.save(session);

		assertNull(altRepository.findById(prevId));
		assertSessionEquals(altRepository.findById(session.getId()));
	}

	@Test
	public void testFindByIndexNameAndIndexValue() {
		testSaveWithPrincipalName();
		Map<String, JdbcSession> result = altRepository.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME,
				"foobar");
		assertEquals(1, result.size());
		assertSessionEquals(session, result.get(session.getId()));
	}

	@Test
	public void testFind2ByIndexNameAndIndexValue() {
		testSaveWithPrincipalName();

		JdbcSession session2 = altRepository.createSession();
		session2.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		altRepository.save(session2);

		Map<String, JdbcSession> result = altRepository.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME,
				"foobar");
		assertEquals(2, result.size());
		assertSessionEquals(result.get(session.getId()));
		assertSessionEquals(session2, result.get(session2.getId()));
	}

	@Test
	public void testEqualEpirations() {
		JdbcSession session2 = altRepository.createSession();
		Instant now = Instant.now();
		session.setLastAccessedTime(now);
		session2.setLastAccessedTime(now);
		altRepository.save(session);
		altRepository.save(session2);

		assertEquals(altRepository.findById(session.getId()).getExpiryTime(),
				altRepository.findById(session2.getId()).getExpiryTime());
	}

	@Test
	public void testCleanUpExpiredSessions() {
		session.setLastAccessedTime(Instant.now().minus(Duration.ofDays(100)));
		altRepository.save(session);
		altRepository.cleanUpExpiredSessions();
		assertEquals(0, (int) template.queryForObject("SELECT COUNT(0) FROM " + DEFAULT_TABLE_NAME, Integer.class));
	}

	@Test
	public void testFindRemovesExpired() {
		session.setLastAccessedTime(Instant.now().minus(Duration.ofDays(100)));
		altRepository.save(session);
		assertNull(altRepository.findById(session.getId()));
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
