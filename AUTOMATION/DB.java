package au.com.coles.otf.utilities;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import au.com.coles.otf.constant.DCCConstants;
import au.com.coles.otf.database_Query.SQLQueries;
import org.apache.log4j.Logger;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DatabaseColumnValidationUtil {

	public static final ObjectMapper mapper = new ObjectMapper();

	private DatabaseColumnValidationUtil() {
		throw new IllegalStateException("Utility class");
	}

	private static Logger logs = Logger.getLogger(DatabaseColumnValidationUtil.class);

	private static boolean validTableDefinition = true;
	private static Connection connection = null;

	public static Connection getConnectionObject() throws SQLException, IOException {
		connection = null;
		if (connection == null) {

			Map<String, String> keyvault = KeyvaultConnectorApplication.getKeyVaultDetails();
			String connection_string = keyvault.get("databaseConnectionString");
			connection = DriverManager.getConnection(connection_string);

		}
		return connection;
	}

	public static boolean closeDBConnectionObject() throws SQLException {
		if (connection != null)
			connection.close();
		return true;
	}

	public static boolean deleteAllTableRecords() throws Exception {
		String sql_1 = "DELETE FROM dcc.dcc_event_handled";
		String sql_2 = "DELETE FROM dcc.dcc_job_queue";
		String sql_3 = "DELETE FROM dcc.dcc_job_queue_arch";
		String sql_4 = "DELETE from dcc.dcc_audit";
		String sql_5 = "DELETE FROM dcc.dcc_active_queue";
		String sql_6 = "DELETE FROM dcc.dcc_event_consumption_stats";
		String sql_7 = "DELETE FROM dcc.dcc_ord_rist_items";
		String sql_8 = "DELETE from dcc.dcc_customer_details";

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(sql_1);
				PreparedStatement readStatement1 = connection.prepareStatement(sql_2);
				PreparedStatement readStatement2 = connection.prepareStatement(sql_3);
				PreparedStatement readStatement3 = connection.prepareStatement(sql_4);
				PreparedStatement readStatement4 = connection.prepareStatement(sql_5);
				PreparedStatement readStatement5 = connection.prepareStatement(sql_6);
				PreparedStatement readStatement6 = connection.prepareStatement(sql_7);
				PreparedStatement readStatement7 = connection.prepareStatement(sql_8);) {

			readStatement.executeUpdate();
			readStatement1.executeUpdate();
			readStatement2.executeUpdate();
			readStatement3.executeUpdate();
			readStatement4.executeUpdate();
			readStatement5.executeUpdate();
			readStatement6.executeUpdate();
			readStatement7.executeUpdate();

			// Thread.sleep(120000);
			return true;
		}

	}

	public static boolean checkDBConnection(String tablename, Map<String, String> tableDescription) throws Exception {

		connection = getConnectionObject();
		logs.info("table name:" + tablename);
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.GET_TABLE_SCHEMA);) {
			readStatement.setString(1, tablename);
			System.out.println("query :" + readStatement.toString());
			try (ResultSet resultSet = readStatement.executeQuery();) {
				Map<String, String> tableColumns = new HashMap<>();

				int count = 0;

				while (resultSet.next()) {
					count++;
					String columnName = resultSet.getString("column_name").toLowerCase();
					String dataType = resultSet.getString("udt_name");
					int length = resultSet.getInt("character_maximum_length");
					if (dataType.equalsIgnoreCase("varchar"))
						tableColumns.put(columnName, dataType + "," + length);
					else
						tableColumns.put(columnName, dataType);

				}
				System.out.println("here table " + tablename + " schema:" + tableColumns);
				if (count == 0) {

					return false;
				}

				validTableDefinition = true;
				if (tableDescription.size() == tableColumns.size()) {
					tableDescription.forEach((key, value) -> {
						key = key.toLowerCase();
						// logs.info("");
						if (!value.equals(tableDescription.get(key))) {

							validTableDefinition = false;
						}
					});
					return validTableDefinition;
				} else {
					return false;
				}

			}
		}
	}

	public static Boolean iseventpersist(String id, String TableName) throws Exception {

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(
				"select count(*) from dcc." + TableName + " as record_count where event_id='" + id + "'");) {
			try (ResultSet resultSet = readStatement.executeQuery();) {
				resultSet.next();
				if (resultSet.getInt(1) > 0)
					return true;
				else
					return false;
			} catch (Exception e) {
				System.out.println("failed for the Table NAme " + TableName + " with excception as " + e);
			}
		}
		return false;
	}

	public static Map<String, String> getdccEventHandledRecord(String event_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.GET_EVENTID);) {
			readStatement.setString(1, event_id);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				while (resultSet.next()) {
					DBRow.put("event_id", resultSet.getString("event_id"));
					DBRow.put("event_datetime", resultSet.getString("event_datetime"));

				}
			}
		}
		logs.info("Database Data:" + DBRow);
		return DBRow;
	}

	public static Map<String, String> getdccjobqueueRecord(String event_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.GET_JOB_QUEUE);) {
			readStatement.setString(1, event_id);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				while (resultSet.next()) {
					DBRow.put("event_id", resultSet.getString("event_id"));
					DBRow.put("event_type", resultSet.getString("event_type"));
					DBRow.put("event_datetime", resultSet.getString("event_datetime"));
					DBRow.put("event_payload", resultSet.getString("event_payload"));
					DBRow.put("poi_event_payload", resultSet.getString("poi_event_payload"));
					DBRow.put("event_operation", resultSet.getString("event_operation"));
					DBRow.put("retry_attempts", resultSet.getString("retry_attempts"));
					DBRow.put("job_set_time", resultSet.getString("job_set_time"));

				}
			}
		}
		logs.info("Database Data:" + DBRow);
		return DBRow;
	}

	public static Map<String, String> getdccjobqueuearchRecord(String event_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.GET_JOB_QUEUE_ARCH);) {
			readStatement.setString(1, event_id);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				while (resultSet.next()) {
					DBRow.put("event_id", resultSet.getString("event_id"));
					DBRow.put("event_type", resultSet.getString("event_type"));
					DBRow.put("event_datetime", resultSet.getString("event_datetime"));
					DBRow.put("event_payload", resultSet.getString("event_payload"));
					DBRow.put("poi_event_payload", resultSet.getString("poi_event_payload"));
					DBRow.put("event_operation", resultSet.getString("event_operation"));
					DBRow.put("retry_attempts", resultSet.getString("retry_attempts"));
					DBRow.put("job_set_time", resultSet.getString("job_set_time"));

				}
			}
		}
		logs.info("Database Data:" + DBRow);
		return DBRow;
	}

	public static Map<String, String> getdccauditRecord(String event_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.GET_AUDIT);) {
			readStatement.setString(1, event_id);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				while (resultSet.next()) {
					DBRow.put("audit_id", resultSet.getString("audit_id"));
					DBRow.put("event_id", resultSet.getString("event_id"));
					DBRow.put("event_operation", resultSet.getString("event_operation"));
					DBRow.put("activity_start_datetime", resultSet.getString("activity_start_datetime"));
					DBRow.put("activity_end_datetime", resultSet.getString("activity_end_datetime"));
					DBRow.put("audit_datetime", resultSet.getString("audit_datetime"));
					DBRow.put("exception", resultSet.getString("exception"));

				}
			}
		}
		logs.info("Database Data:" + DBRow);
		return DBRow;
	}

	public static Map<String, String> getordlistRecord(String event_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.ORD_LIST);) {
			readStatement.setString(1, event_id);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				while (resultSet.next()) {
					DBRow.put("order_id", resultSet.getString("order_id"));
					DBRow.put("customer_id", resultSet.getString("customer_id"));
					DBRow.put("fulfilment_id", resultSet.getString("fulfilment_id"));
					DBRow.put("item_id", resultSet.getString("item_id"));
					DBRow.put("is_liq", resultSet.getString("is_liq"));
					DBRow.put("store_id", resultSet.getString("store_id"));
					DBRow.put("delivery_datetime", resultSet.getString("delivery_datetime"));
					DBRow.put("created_datetime", resultSet.getString("created_datetime"));

				}
			}
		}
		logs.info("Database Data:" + DBRow);
		return DBRow;
	}

	public static Boolean getdccauditOps(String event_id, String OPERATION) throws SQLException, IOException {
		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection
				.prepareStatement("select count(*) from dcc.dcc_audit where event_id='" + event_id
						+ "' and event_operation='" + OPERATION + "'");) {
			try (ResultSet resultSet = readStatement.executeQuery();) {
				resultSet.next();
				if (resultSet.getInt(1) > 0)
					return true;
				else
					return false;
			} catch (Exception e) {
				System.out.println(
						"failed for the while fetching operation Name " + OPERATION + " with excception as " + e);
			}
		}
		return false;
	}

	public static boolean recordcountindccaudit(String eventId) throws Exception {

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection
				.prepareStatement(SQLQueries.GET_RECORD_COUNT_IN_DCC_AUDIT_BY_EVENT_ID);) {
			readStatement.setString(1, eventId);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				resultSet.next();
				return (resultSet.getInt(1) >= 2);
			}
		}

	}

	public static boolean recordcountindccqueuearch(String eventId) throws Exception {

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection
				.prepareStatement(SQLQueries.GET_RECORD_COUNT_IN_DCC_QUEUE_BY_EVENT_ID);) {
			readStatement.setString(1, eventId);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				resultSet.next();
				return (resultSet.getInt(1) >= 1);
			}
		}

	}

	public static boolean recordcountindcchandled(String eventId) throws Exception {

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection
				.prepareStatement(SQLQueries.GET_RECORD_COUNT_IN_DCC_HANDLED_BY_EVENT_ID);) {
			readStatement.setString(1, eventId);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				resultSet.next();
				return (resultSet.getInt(1) >= 1);
			}
		}

	}

	public static int exceptionindccaudit(String eventId) throws Exception {

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection
				.prepareStatement(SQLQueries.GET_EXCEPTION_COUNT_IN_DCC_AUDIT_BY_EVENT_ID);) {
			readStatement.setString(1, eventId);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				resultSet.next();
				return (resultSet.getInt(1));
			}
		}

	}

	public static Map<String, String> getactivequeuerecord(String event_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.GET_ACTIVE_QUEUE);) {
			readStatement.setString(1, event_id);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				while (resultSet.next()) {
					DBRow.put("event_id", resultSet.getString("event_id"));
					DBRow.put("created_datetimestamp", resultSet.getString("created_datetimestamp"));
				}
			}
		}
		logs.info("Database Data:" + DBRow);
		return DBRow;
	}

	public static Map<String, String> getcustdetailsrecord(String customer_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.GET_CUSTOMER_DETAILS);) {
			readStatement.setString(1, customer_id);
			try (ResultSet resultSet = readStatement.executeQuery();) {
				while (resultSet.next()) {
					DBRow.put("customer_id", resultSet.getString("customer_id"));
					DBRow.put("liq_flag", resultSet.getString("liq_flag"));
					DBRow.put("order_id", resultSet.getString("order_id"));
					DBRow.put("store_id", resultSet.getString("store_id"));
					DBRow.put("last_update_dateime", resultSet.getString("last_update_dateime"));
				}
			}
		}
		logs.info("Database Data:" + DBRow);
		return DBRow;
	}

	public static Boolean insertcustdetailsrecord(String customer_id) throws Exception {
		Map<String, String> DBRow = new HashMap<>();

		connection = getConnectionObject();
		try (PreparedStatement readStatement = connection.prepareStatement(SQLQueries.INSERT_CUSTOMER_RECORD);) {
			readStatement.setString(1, customer_id);
			readStatement.execute();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	//////////////////////////////////////////////////////

//	public static Map<String, String> fetchDbData(String event_id) throws Exception {
//		 
//	    Map<String, String> DBRow = new HashMap<>();
//	 
//	    try (Connection connection = getConnectionObject();
//	         PreparedStatement ps =
//	             connection.prepareStatement(SQLQueries.GET_DB_DATA)) {
//	 
//	        ps.setString(1, event_id);
//	 
//	        try (ResultSet rs = ps.executeQuery()) {
//	 
//	            if (!rs.next()) {
//	                throw new RuntimeException("No DB record found for ID: " + event_id);
//	            }
//	 
//	            ResultSetMetaData metaData = rs.getMetaData();
//	            int columnCount = metaData.getColumnCount();
//	 
//	            for (int i = 1; i <= columnCount; i++) {
//	                String columnName = metaData.getColumnName(i);
//	                String columnValue = rs.getString(i); // JSON or normal string
//	                DBRow.put(columnName, columnValue);
//	            }
//	        }
//	    }
//	 
//	    logs.info("Database Data: " + DBRow);
//	    return DBRow;
//	}
//	

	public static ObjectNode fetchDbDataAsJson(String eventId) throws Exception {
		 
	    ObjectMapper mapper = new ObjectMapper();
	    ObjectNode dbJson = mapper.createObjectNode();
	 
	    try (Connection connection = getConnectionObject();
	         PreparedStatement ps =
	             connection.prepareStatement(SQLQueries.GET_DB_DATA)) {
	 
	        ps.setString(1, eventId);
	 
	        try (ResultSet rs = ps.executeQuery()) {
	 
	            if (!rs.next()) {
	                throw new RuntimeException(
	                    "No DB record found for event_id: " + eventId);
	            }
	 
	            ResultSetMetaData meta = rs.getMetaData();
	            int columnCount = meta.getColumnCount();
	 
	            for (int i = 1; i <= columnCount; i++) {
	 
	                String columnName = meta.getColumnName(i);
	                String dbValue = rs.getString(i);
	 
	                if (dbValue == null) {
	                    dbJson.set(columnName, NullNode.getInstance());
	                } else if (looksLikeJson(dbValue)) {
	                    dbJson.set(columnName, mapper.readTree(dbValue));
	                } else {
	                    dbJson.put(columnName, dbValue);
	                }
	            }
	        }
	    }
	    return dbJson;
	}
	 
	
	private static boolean looksLikeJson(String value) {
	    return value.trim().startsWith("{") || value.trim().startsWith("[");
	}
	 
	
	    public static List<Map<String, Object>> fetchByLookup(
	            String tableName,
	            String idColumn,
	            String idValue,
	            String orderIdColumn,
	            String orderIdValue
	    ) throws SQLException, IOException {
	        List<Map<String, Object>> rows = new ArrayList<>();
	        if ((idColumn == null || idColumn.isEmpty()) && (orderIdColumn == null || orderIdColumn.isEmpty())) {
	            throw new IllegalArgumentException("No lookup columns provided for table " + tableName);
	        }

	        StringBuilder query = new StringBuilder("SELECT * FROM dcc." + tableName + " WHERE ");
	        List<String> cols = new ArrayList<>();
	        List<String> vals = new ArrayList<>();
	        if (idColumn != null && !idColumn.isEmpty() && idValue != null && !idValue.isEmpty()) {
	            cols.add(idColumn);
	            vals.add(idValue);
	        }
	        if (orderIdColumn != null && !orderIdColumn.isEmpty() && orderIdValue != null && !orderIdValue.isEmpty()) {
	            cols.add(orderIdColumn);
	            vals.add(orderIdValue);
	        }
	        if (cols.isEmpty()) {
	            throw new IllegalArgumentException("No lookup values available for table " + tableName);
	        }
	        for (int i = 0; i < cols.size(); i++) {
	            if (i > 0) query.append(" OR ");
	            query.append(cols.get(i)).append(" = ?");
	        }
	        
	        

//	        String jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false&allowPublicKeyRetrieval=true";
	        // logs suppressed
	        try (Connection conn = getConnectionObject();
	             PreparedStatement stmt = conn.prepareStatement(query.toString())) {
	            for (int i = 0; i < vals.size(); i++) {
	                stmt.setString(i + 1, vals.get(i));
	            }
	            try (ResultSet rs = stmt.executeQuery()) {
	                ResultSetMetaData meta = rs.getMetaData();
	                int colCount = meta.getColumnCount();
	                while (rs.next()) {
	                    Map<String, Object> row = new HashMap<>();
	                    for (int i = 1; i <= colCount; i++) {
	                        String colName = meta.getColumnLabel(i);
	                        row.put(colName, rs.getObject(i));
	                    }
	                    rows.add(row);
	                }
	            }
	        }
	        return rows;
	    }

	    public static List<String> listTables() throws SQLException, IOException {
	        List<String> tables = new ArrayList<>();
//	        String jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false&allowPublicKeyRetrieval=true";
	        String query = "select table_name from INFORMATION_SCHEMA.tables where table_schema='dcc'";
	        try (Connection conn =getConnectionObject();
	             PreparedStatement stmt = conn.prepareStatement(query);
	             ResultSet rs = stmt.executeQuery()) {
	            while (rs.next()) {
	                tables.add(rs.getString(1));
	            }
	        }
	        return tables;
	    }

	    public List<Map<String, Object>> fetchByIdOrOrderId(
	            String tableName,
	            String idColumn,
	            String orderIdColumn,
	            String idValue,
	            String orderIdValue
	    ) throws SQLException, IOException {
	        String query = "SELECT * FROM " + tableName + " WHERE " + idColumn + " = ? OR " + orderIdColumn + " = ?";
	        List<Map<String, Object>> rows = new ArrayList<>();

//	        String jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false&allowPublicKeyRetrieval=true";
	        // logs suppressed
	        try (Connection conn = getConnectionObject();
	             PreparedStatement stmt = conn.prepareStatement(query)) {
	            stmt.setString(1, idValue);
	            stmt.setString(2, orderIdValue);
	            try (ResultSet rs = stmt.executeQuery()) {
	                ResultSetMetaData meta = rs.getMetaData();
	                int colCount = meta.getColumnCount();
	                while (rs.next()) {
	                    Map<String, Object> row = new HashMap<>();
	                    for (int i = 1; i <= colCount; i++) {
	                        String colName = meta.getColumnLabel(i);
	                        row.put(colName, rs.getObject(i));
	                    }
	                    rows.add(row);
	                }
	            }
	        }
	        return rows;
	    }
}
