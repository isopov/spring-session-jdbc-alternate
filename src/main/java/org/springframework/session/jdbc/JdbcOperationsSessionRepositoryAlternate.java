/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.session.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.dao.DataAccessException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class JdbcOperationsSessionRepositoryAlternate
		implements FindByIndexNameSessionRepository<JdbcOperationsSessionRepositoryAlternate.JdbcSession> {

	/**
	 * The default name of database table used by Spring Session to store sessions.
	 */
	public static final String DEFAULT_TABLE_NAME = "SPRING_SESSION_ALT";

	private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

	private static final String CREATE_SESSION_QUERY = "INSERT INTO %TABLE_NAME%(SESSION_ID1, SESSION_ID2, CREATION_TIME, LAST_ACCESS_TIME, MAX_INACTIVE_INTERVAL, EXPIRY_TIME, PRINCIPAL_NAME) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?)";

	private static final String CREATE_SESSION_ATTRIBUTE_QUERY = "INSERT INTO %TABLE_NAME%_ATTRIBUTES(SESSION_ID1, SESSION_ID2, ATTRIBUTE_NAME, ATTRIBUTE_BYTES) "
			+ "VALUES (?, ?, ?, ?)";

	private static final String GET_SESSION_QUERY = "SELECT S.SESSION_ID1, S.SESSION_ID2, S.CREATION_TIME, S.LAST_ACCESS_TIME, S.MAX_INACTIVE_INTERVAL, SA.ATTRIBUTE_NAME, SA.ATTRIBUTE_BYTES "
			+ "FROM %TABLE_NAME% S "
			+ "LEFT OUTER JOIN %TABLE_NAME%_ATTRIBUTES SA ON S.SESSION_ID1 = SA.SESSION_ID1 AND S.SESSION_ID2 = SA.SESSION_ID2 "
			+ "WHERE S.SESSION_ID1 = ? AND S.SESSION_ID2 = ?";

	private static final String UPDATE_SESSION_QUERY = "UPDATE %TABLE_NAME% SET SESSION_ID1 = ?, SESSION_ID2 = ?, LAST_ACCESS_TIME = ?, MAX_INACTIVE_INTERVAL = ?, EXPIRY_TIME = ?, PRINCIPAL_NAME = ? "
			+ "WHERE SESSION_ID1 = ? AND SESSION_ID2 = ?";

	private static final String UPDATE_SESSION_ATTRIBUTE_QUERY = "UPDATE %TABLE_NAME%_ATTRIBUTES SET ATTRIBUTE_BYTES = ? "
			+ "WHERE SESSION_ID1 = ? AND SESSION_ID2 = ? " + "AND ATTRIBUTE_NAME = ?";

	private static final String DELETE_SESSION_ATTRIBUTE_QUERY = "DELETE FROM %TABLE_NAME%_ATTRIBUTES "
			+ "WHERE SESSION_ID1 = ? AND SESSION_ID2 = ? " + "AND ATTRIBUTE_NAME = ?";

	private static final String DELETE_SESSION_QUERY = "DELETE FROM %TABLE_NAME% "
			+ "WHERE SESSION_ID1 = ? AND SESSION_ID2 = ?";

	private static final String LIST_SESSIONS_BY_PRINCIPAL_NAME_QUERY = "SELECT S.SESSION_ID1, S.SESSION_ID2, S.CREATION_TIME, S.LAST_ACCESS_TIME, S.MAX_INACTIVE_INTERVAL, SA.ATTRIBUTE_NAME, SA.ATTRIBUTE_BYTES "
			+ "FROM %TABLE_NAME% S "
			+ "LEFT OUTER JOIN %TABLE_NAME%_ATTRIBUTES SA ON S.SESSION_ID1 = SA.SESSION_ID1 AND S.SESSION_ID2 = SA.SESSION_ID2 "
			+ "WHERE S.PRINCIPAL_NAME = ?";

	private static final String DELETE_SESSIONS_BY_EXPIRY_TIME_QUERY = "DELETE FROM %TABLE_NAME% "
			+ "WHERE EXPIRY_TIME < ?";

	private static final Log logger = LogFactory.getLog(JdbcOperationsSessionRepositoryAlternate.class);

	private static final PrincipalNameResolver PRINCIPAL_NAME_RESOLVER = new PrincipalNameResolver();

	private final JdbcOperations jdbcOperations;

	private final TransactionOperations transactionOperations;

	private final ResultSetExtractor<List<JdbcSession>> extractor = new SessionResultSetExtractor();

	/**
	 * The name of database table used by Spring Session to store sessions.
	 */
	private String tableName = DEFAULT_TABLE_NAME;

	private String createSessionQuery;

	private String createSessionAttributeQuery;

	private String getSessionQuery;

	private String updateSessionQuery;

	private String updateSessionAttributeQuery;

	private String deleteSessionAttributeQuery;

	private String deleteSessionQuery;

	private String listSessionsByPrincipalNameQuery;

	private String deleteSessionsByExpiryTimeQuery;

	/**
	 * If non-null, this value is used to override the default value for
	 * {@link JdbcSession#setMaxInactiveInterval(Duration)}.
	 */
	private Integer defaultMaxInactiveInterval;

	private ConversionService conversionService;

	private LobHandler lobHandler = new DefaultLobHandler();

	/**
	 * Create a new {@link JdbcOperationsSessionRepositoryAlternate} instance which
	 * uses the provided {@link JdbcOperations} to manage sessions.
	 * 
	 * @param jdbcOperations
	 *            the {@link JdbcOperations} to use
	 * @param transactionManager
	 *            the {@link PlatformTransactionManager} to use
	 */
	public JdbcOperationsSessionRepositoryAlternate(JdbcOperations jdbcOperations,
			PlatformTransactionManager transactionManager) {
		Assert.notNull(jdbcOperations, "JdbcOperations must not be null");
		this.jdbcOperations = jdbcOperations;
		this.transactionOperations = createTransactionTemplate(transactionManager);
		this.conversionService = createDefaultConversionService();
		prepareQueries();
	}

	/**
	 * Set the name of database table used to store sessions.
	 * 
	 * @param tableName
	 *            the database table name
	 */
	public void setTableName(String tableName) {
		Assert.hasText(tableName, "Table name must not be empty");
		this.tableName = tableName.trim();
		prepareQueries();
	}

	/**
	 * Set the custom SQL query used to create the session.
	 * 
	 * @param createSessionQuery
	 *            the SQL query string
	 */
	public void setCreateSessionQuery(String createSessionQuery) {
		Assert.hasText(createSessionQuery, "Query must not be empty");
		this.createSessionQuery = createSessionQuery;
	}

	/**
	 * Set the custom SQL query used to create the session attribute.
	 * 
	 * @param createSessionAttributeQuery
	 *            the SQL query string
	 */
	public void setCreateSessionAttributeQuery(String createSessionAttributeQuery) {
		Assert.hasText(createSessionAttributeQuery, "Query must not be empty");
		this.createSessionAttributeQuery = createSessionAttributeQuery;
	}

	/**
	 * Set the custom SQL query used to retrieve the session.
	 * 
	 * @param getSessionQuery
	 *            the SQL query string
	 */
	public void setGetSessionQuery(String getSessionQuery) {
		Assert.hasText(getSessionQuery, "Query must not be empty");
		this.getSessionQuery = getSessionQuery;
	}

	/**
	 * Set the custom SQL query used to update the session.
	 * 
	 * @param updateSessionQuery
	 *            the SQL query string
	 */
	public void setUpdateSessionQuery(String updateSessionQuery) {
		Assert.hasText(updateSessionQuery, "Query must not be empty");
		this.updateSessionQuery = updateSessionQuery;
	}

	/**
	 * Set the custom SQL query used to update the session attribute.
	 * 
	 * @param updateSessionAttributeQuery
	 *            the SQL query string
	 */
	public void setUpdateSessionAttributeQuery(String updateSessionAttributeQuery) {
		Assert.hasText(updateSessionAttributeQuery, "Query must not be empty");
		this.updateSessionAttributeQuery = updateSessionAttributeQuery;
	}

	/**
	 * Set the custom SQL query used to delete the session attribute.
	 * 
	 * @param deleteSessionAttributeQuery
	 *            the SQL query string
	 */
	public void setDeleteSessionAttributeQuery(String deleteSessionAttributeQuery) {
		Assert.hasText(deleteSessionAttributeQuery, "Query must not be empty");
		this.deleteSessionAttributeQuery = deleteSessionAttributeQuery;
	}

	/**
	 * Set the custom SQL query used to delete the session.
	 * 
	 * @param deleteSessionQuery
	 *            the SQL query string
	 */
	public void setDeleteSessionQuery(String deleteSessionQuery) {
		Assert.hasText(deleteSessionQuery, "Query must not be empty");
		this.deleteSessionQuery = deleteSessionQuery;
	}

	/**
	 * Set the custom SQL query used to retrieve the sessions by principal name.
	 * 
	 * @param listSessionsByPrincipalNameQuery
	 *            the SQL query string
	 */
	public void setListSessionsByPrincipalNameQuery(String listSessionsByPrincipalNameQuery) {
		Assert.hasText(listSessionsByPrincipalNameQuery, "Query must not be empty");
		this.listSessionsByPrincipalNameQuery = listSessionsByPrincipalNameQuery;
	}

	/**
	 * Set the custom SQL query used to delete the sessions by last access time.
	 * 
	 * @param deleteSessionsByExpiryTimeQuery
	 *            the SQL query string
	 */
	public void setDeleteSessionsByExpiryTimeQuery(String deleteSessionsByExpiryTimeQuery) {
		Assert.hasText(deleteSessionsByExpiryTimeQuery, "Query must not be empty");
		this.deleteSessionsByExpiryTimeQuery = deleteSessionsByExpiryTimeQuery;
	}

	/**
	 * Set the maximum inactive interval in seconds between requests before newly
	 * created sessions will be invalidated. A negative time indicates that the
	 * session will never timeout. The default is 1800 (30 minutes).
	 * 
	 * @param defaultMaxInactiveInterval
	 *            the maximum inactive interval in seconds
	 */
	public void setDefaultMaxInactiveInterval(Integer defaultMaxInactiveInterval) {
		this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
	}

	public void setLobHandler(LobHandler lobHandler) {
		Assert.notNull(lobHandler, "LobHandler must not be null");
		this.lobHandler = lobHandler;
	}

	/**
	 * Sets the {@link ConversionService} to use.
	 * 
	 * @param conversionService
	 *            the converter to set
	 */
	public void setConversionService(ConversionService conversionService) {
		Assert.notNull(conversionService, "conversionService must not be null");
		this.conversionService = conversionService;
	}

	@Override
	public JdbcSession createSession() {
		JdbcSession session = new JdbcSession();
		if (this.defaultMaxInactiveInterval != null) {
			session.setMaxInactiveInterval(Duration.ofSeconds(this.defaultMaxInactiveInterval));
		}
		return session;
	}

	@Override
	public void save(final JdbcSession session) {
		if (session.isNew()) {
			this.transactionOperations.execute(new TransactionCallbackWithoutResult() {

				@Override
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations
							.update(JdbcOperationsSessionRepositoryAlternate.this.createSessionQuery, ps -> {
								ps.setLong(1, session.id.getMostSignificantBits());
								ps.setLong(2, session.id.getLeastSignificantBits());
								ps.setLong(3, session.getCreationTime().toEpochMilli());
								ps.setLong(4, session.getLastAccessedTime().toEpochMilli());
								ps.setInt(5, (int) session.getMaxInactiveInterval().getSeconds());
								ps.setLong(6, session.getExpiryTime().toEpochMilli());
								ps.setString(7, session.getPrincipalName());
							});
					if (!session.getAttributeNames().isEmpty()) {
						final List<String> attributeNames = new ArrayList<>(session.getAttributeNames());
						JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations.batchUpdate(
								JdbcOperationsSessionRepositoryAlternate.this.createSessionAttributeQuery,
								new BatchPreparedStatementSetter() {

									@Override
									public void setValues(PreparedStatement ps, int i) throws SQLException {
										String attributeName = attributeNames.get(i);
										ps.setLong(1, session.id.getMostSignificantBits());
										ps.setLong(2, session.id.getLeastSignificantBits());
										ps.setString(3, attributeName);
										serialize(ps, 4, session.getAttribute(attributeName));
									}

									@Override
									public int getBatchSize() {
										return attributeNames.size();
									}

								});
					}
				}

			});
		} else {
			this.transactionOperations.execute(new TransactionCallbackWithoutResult() {

				@Override
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					if (session.isChanged()) {
						JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations
								.update(JdbcOperationsSessionRepositoryAlternate.this.updateSessionQuery, ps -> {
									ps.setLong(1, session.id.getMostSignificantBits());
									ps.setLong(2, session.id.getLeastSignificantBits());
									ps.setLong(3, session.getLastAccessedTime().toEpochMilli());
									ps.setInt(4, (int) session.getMaxInactiveInterval().getSeconds());
									ps.setLong(5, session.getExpiryTime().toEpochMilli());
									ps.setString(6, session.getPrincipalName());
									if (session.prevId != null) {
										ps.setLong(7, session.prevId.getMostSignificantBits());
										ps.setLong(8, session.prevId.getLeastSignificantBits());
										session.prevId = null;
									} else {
										ps.setLong(7, session.id.getMostSignificantBits());
										ps.setLong(8, session.id.getLeastSignificantBits());
									}

								});
					}
					Map<String, Object> delta = session.getDelta();
					if (!delta.isEmpty()) {
						for (final Map.Entry<String, Object> entry : delta.entrySet()) {
							if (entry.getValue() == null) {
								JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations.update(
										JdbcOperationsSessionRepositoryAlternate.this.deleteSessionAttributeQuery,
										ps -> {
											ps.setLong(1, session.id.getMostSignificantBits());
											ps.setLong(2, session.id.getLeastSignificantBits());
											ps.setString(3, entry.getKey());
										});
							} else {
								int updatedCount = JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations.update(
										JdbcOperationsSessionRepositoryAlternate.this.updateSessionAttributeQuery,
										ps -> {
											serialize(ps, 1, entry.getValue());
											ps.setLong(2, session.id.getMostSignificantBits());
											ps.setLong(3, session.id.getLeastSignificantBits());
											ps.setString(4, entry.getKey());
										});
								if (updatedCount == 0) {
									JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations.update(
											JdbcOperationsSessionRepositoryAlternate.this.createSessionAttributeQuery,
											ps -> {
												ps.setLong(1, session.id.getMostSignificantBits());
												ps.setLong(2, session.id.getLeastSignificantBits());
												ps.setString(3, entry.getKey());
												serialize(ps, 4, entry.getValue());
											});
								}
							}
						}
					}
				}

			});
		}
		session.clearChangeFlags();
	}

	@Override
	public JdbcSession findById(final String id) {
		return findById(UUID.fromString(id));
	}

	private JdbcSession findById(UUID id) {
		final JdbcSession session = this.transactionOperations.execute(status -> {
			List<JdbcSession> sessions = JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations
					.query(JdbcOperationsSessionRepositoryAlternate.this.getSessionQuery, ps -> {
						ps.setLong(1, id.getMostSignificantBits());
						ps.setLong(2, id.getLeastSignificantBits());
					}, JdbcOperationsSessionRepositoryAlternate.this.extractor);
			if (sessions.isEmpty()) {
				return null;
			}
			return sessions.get(0);
		});

		if (session != null) {
			if (session.isExpired()) {
				deleteById(id);
			} else {
				return session;
			}
		}
		return null;
	}

	@Override
	public void deleteById(final String id) {
		deleteById(UUID.fromString(id));
	}

	private void deleteById(UUID id) {
		this.transactionOperations.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations.update(
						JdbcOperationsSessionRepositoryAlternate.this.deleteSessionQuery, id.getMostSignificantBits(),
						id.getLeastSignificantBits());
			}

		});
	}

	@Override
	public Map<String, JdbcSession> findByIndexNameAndIndexValue(String indexName, final String indexValue) {
		if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
			return Collections.emptyMap();
		}

		List<JdbcSession> sessions = this.transactionOperations
				.execute(status -> JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations.query(
						JdbcOperationsSessionRepositoryAlternate.this.listSessionsByPrincipalNameQuery,
						ps -> ps.setString(1, indexValue), JdbcOperationsSessionRepositoryAlternate.this.extractor));

		Map<String, JdbcSession> sessionMap = new HashMap<>(sessions.size());

		for (JdbcSession session : sessions) {
			sessionMap.put(session.getId(), session);
		}

		return sessionMap;
	}

	public void cleanUpExpiredSessions() {
		Integer deletedCount = this.transactionOperations
				.execute(transactionStatus -> JdbcOperationsSessionRepositoryAlternate.this.jdbcOperations.update(
						JdbcOperationsSessionRepositoryAlternate.this.deleteSessionsByExpiryTimeQuery,
						System.currentTimeMillis()));

		if (logger.isDebugEnabled()) {
			logger.debug("Cleaned up " + deletedCount + " expired sessions");
		}
	}

	private static TransactionTemplate createTransactionTemplate(PlatformTransactionManager transactionManager) {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		transactionTemplate.afterPropertiesSet();
		return transactionTemplate;
	}

	private static GenericConversionService createDefaultConversionService() {
		GenericConversionService converter = new GenericConversionService();
		converter.addConverter(Object.class, byte[].class, new SerializingConverter());
		converter.addConverter(byte[].class, Object.class, new DeserializingConverter());
		return converter;
	}

	private String getQuery(String base) {
		return StringUtils.replace(base, "%TABLE_NAME%", this.tableName);
	}

	private void prepareQueries() {
		this.createSessionQuery = getQuery(CREATE_SESSION_QUERY);
		this.createSessionAttributeQuery = getQuery(CREATE_SESSION_ATTRIBUTE_QUERY);
		this.getSessionQuery = getQuery(GET_SESSION_QUERY);
		this.updateSessionQuery = getQuery(UPDATE_SESSION_QUERY);
		this.updateSessionAttributeQuery = getQuery(UPDATE_SESSION_ATTRIBUTE_QUERY);
		this.deleteSessionAttributeQuery = getQuery(DELETE_SESSION_ATTRIBUTE_QUERY);
		this.deleteSessionQuery = getQuery(DELETE_SESSION_QUERY);
		this.listSessionsByPrincipalNameQuery = getQuery(LIST_SESSIONS_BY_PRINCIPAL_NAME_QUERY);
		this.deleteSessionsByExpiryTimeQuery = getQuery(DELETE_SESSIONS_BY_EXPIRY_TIME_QUERY);
	}

	private void serialize(PreparedStatement ps, int paramIndex, Object attributeValue) throws SQLException {
		this.lobHandler.getLobCreator().setBlobAsBytes(ps, paramIndex, (byte[]) this.conversionService
				.convert(attributeValue, TypeDescriptor.valueOf(Object.class), TypeDescriptor.valueOf(byte[].class)));
	}

	private Object deserialize(ResultSet rs, String columnName) throws SQLException {
		return this.conversionService.convert(this.lobHandler.getBlobAsBytes(rs, columnName),
				TypeDescriptor.valueOf(byte[].class), TypeDescriptor.valueOf(Object.class));
	}

	/**
	 * The {@link Session} to use for
	 * {@link JdbcOperationsSessionRepositoryAlternate}.
	 *
	 * @author Vedran Pavic
	 */
	final class JdbcSession implements Session {

		private final MapSession delegate;

		private UUID prevId;

		private UUID id;

		private boolean isNew;

		private boolean changed;

		private Map<String, Object> delta = new HashMap<>();

		JdbcSession() {
			this.id = UUID.randomUUID();
			this.delegate = new MapSession(id.toString());
			this.isNew = true;
		}

		JdbcSession(UUID id, MapSession delegate) {
			Assert.notNull(id, "id cannot be null");
			Assert.notNull(delegate, "Session cannot be null");
			this.id = id;
			this.delegate = delegate;
		}

		boolean isNew() {
			return this.isNew;
		}

		boolean isChanged() {
			return this.changed;
		}

		Map<String, Object> getDelta() {
			return this.delta;
		}

		void clearChangeFlags() {
			this.isNew = false;
			this.changed = false;
			this.delta.clear();
		}

		String getPrincipalName() {
			return PRINCIPAL_NAME_RESOLVER.resolvePrincipal(this);
		}

		Instant getExpiryTime() {
			return getLastAccessedTime().plus(getMaxInactiveInterval());
		}

		@Override
		public String getId() {
			return this.delegate.getId();
		}

		@Override
		public String changeSessionId() {
			this.changed = true;
			this.prevId = id;
			this.id = UUID.randomUUID();
			String changed = id.toString();
			this.delegate.setId(changed);
			return changed;
		}

		@Override
		public <T> T getAttribute(String attributeName) {
			return this.delegate.getAttribute(attributeName);
		}

		@Override
		public Set<String> getAttributeNames() {
			return this.delegate.getAttributeNames();
		}

		@Override
		public void setAttribute(String attributeName, Object attributeValue) {
			this.delegate.setAttribute(attributeName, attributeValue);
			this.delta.put(attributeName, attributeValue);
			if (PRINCIPAL_NAME_INDEX_NAME.equals(attributeName) || SPRING_SECURITY_CONTEXT.equals(attributeName)) {
				this.changed = true;
			}
		}

		@Override
		public void removeAttribute(String attributeName) {
			this.delegate.removeAttribute(attributeName);
			this.delta.put(attributeName, null);
		}

		@Override
		public Instant getCreationTime() {
			return this.delegate.getCreationTime();
		}

		@Override
		public void setLastAccessedTime(Instant lastAccessedTime) {
			this.delegate.setLastAccessedTime(lastAccessedTime);
			this.changed = true;
		}

		@Override
		public Instant getLastAccessedTime() {
			return this.delegate.getLastAccessedTime();
		}

		@Override
		public void setMaxInactiveInterval(Duration interval) {
			this.delegate.setMaxInactiveInterval(interval);
			this.changed = true;
		}

		@Override
		public Duration getMaxInactiveInterval() {
			return this.delegate.getMaxInactiveInterval();
		}

		@Override
		public boolean isExpired() {
			return this.delegate.isExpired();
		}

	}

	/**
	 * Resolves the Spring Security principal name.
	 *
	 * @author Vedran Pavic
	 */
	static class PrincipalNameResolver {

		private SpelExpressionParser parser = new SpelExpressionParser();

		public String resolvePrincipal(Session session) {
			String principalName = session.getAttribute(PRINCIPAL_NAME_INDEX_NAME);
			if (principalName != null) {
				return principalName;
			}
			Object authentication = session.getAttribute(SPRING_SECURITY_CONTEXT);
			if (authentication != null) {
				Expression expression = this.parser.parseExpression("authentication?.name");
				return expression.getValue(authentication, String.class);
			}
			return null;
		}

	}

	private class SessionResultSetExtractor implements ResultSetExtractor<List<JdbcSession>> {

		@Override
		public List<JdbcSession> extractData(ResultSet rs) throws SQLException, DataAccessException {
			List<JdbcSession> sessions = new ArrayList<>();
			while (rs.next()) {
				UUID id = new UUID(rs.getLong("SESSION_ID1"), rs.getLong("SESSION_ID2"));
				JdbcSession session;
				if (sessions.size() > 0 && getLast(sessions).id.equals(id)) {
					session = getLast(sessions);
				} else {
					MapSession delegate = new MapSession(id.toString());
					delegate.setCreationTime(Instant.ofEpochMilli(rs.getLong("CREATION_TIME")));
					delegate.setLastAccessedTime(Instant.ofEpochMilli(rs.getLong("LAST_ACCESS_TIME")));
					delegate.setMaxInactiveInterval(Duration.ofSeconds(rs.getInt("MAX_INACTIVE_INTERVAL")));
					session = new JdbcSession(id, delegate);
				}
				String attributeName = rs.getString("ATTRIBUTE_NAME");
				if (attributeName != null) {
					session.setAttribute(attributeName, deserialize(rs, "ATTRIBUTE_BYTES"));
				}
				sessions.add(session);
			}
			return sessions;
		}

		private JdbcSession getLast(List<JdbcSession> sessions) {
			return sessions.get(sessions.size() - 1);
		}

	}

}
