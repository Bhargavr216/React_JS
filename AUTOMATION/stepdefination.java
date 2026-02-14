package au.com.coles.otf.stepdefinitions;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;

import au.com.coles.otf.utilities.KeyvaultConnectorApplication;
import au.com.coles.otf.constant.DCCConstants;
import au.com.coles.otf.utilities.DatabaseColumnValidationUtil;
import au.com.coles.otf.utilities.EventTriggerUtil;
import au.com.coles.otf.utilities.JsonCompare;
import au.com.coles.otf.utilities.JsonCompare.ValidationReport;
import au.com.coles.otf.utilities.JsonFileReader;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.qameta.allure.Allure;
import io.swagger.v3.oas.models.SpecVersion;

import static org.junit.Assert.*;

public class DCC {

	private static Logger logs = Logger.getLogger(DCC.class);
	private static String[] TABLENAME = new String[] { "mfs_audit", "mfs_fulfilment_version", "mfs_job_queue",
			"ofeh_event_handled", "ofeh_audit", "ofeh_job_queue", "mfs_job_queue_arch", "ofeh_job_queue_arch" };
	public static Map<String, String> keyVault;
	private static String dbConnectionString;
	private static Connection connection = null;
	private static PreparedStatement stmt = null;
	private String eventType = "";
	
	 private final ObjectMapper mapper = new ObjectMapper();	   
	    private final JsonCompare jsonCompare = new JsonCompare();

	 

	    private String payloadPath;
	    private String expectedPath;
	    private String schemaDir;
	    private JsonNode payloadArray;
	    private JsonNode payloadArray2;
	    private String selectedEventId;
	    private String selectedOrderId;
	    private final StringBuilder scenarioLog = new StringBuilder();

	    private final List<ValidationReport> reports = new ArrayList<>();
	    private final Map<String, JsonCompare.ColumnRule> columnRuleCache = new HashMap<>();

	@Before(value = "@MFS")
	public static void beforeScenario() throws SQLException {
		try {

			keyVault = KeyvaultConnectorApplication.getKeyVaultDetails();
			dbConnectionString = keyVault.get("databaseConnectionString");
			connection = DriverManager.getConnection(dbConnectionString);
			for (int i = 0; i < TABLENAME.length; i++) {

				/*
				 * String query = "truncate table ?"; query = query.replace("?", TABLENAME[i]);
				 * stmt = connection.prepareStatement(query); stmt.execute();
				 * logs.info("Table Truncated");
				 */

			}
		} catch (Exception e) {
			logs.error("error handled in catch block");
		} finally {
			if (connection != null)
				connection.close();
			if (stmt != null)
				stmt.close();
		}
	}

	@Given("Azure Event Hub receives the event of {string} and {string}")
	public void azure_event_hub_receives_the_event_of_and(String eventTypeName, String testDataPath) {
		try {
			eventType = eventTypeName;
			assertTrue("Event is not triggered..", EventTriggerUtil.eventTrigger(testDataPath));
			logs.info(eventTypeName + " Event is triggered");
//			Thread.sleep(6000);

		} catch (Exception e) {
			fail(e.getMessage());
			e.printStackTrace();

		}
	}

	@And("Validate {string} table schema")
	public void validate_table_schema(String tableName) {
		try {
			Map<String, String> tableDescription = new HashMap<>();
			if (tableName.equalsIgnoreCase("dcc_event_handled")) {

				tableDescription.put("event_id", DCCConstants.VARCHARH);
				tableDescription.put("event_datetime", DCCConstants.TIMESTAMPZ);

			} else if (tableName.equalsIgnoreCase("dcc_job_queue")) {
				tableDescription.put("event_id", DCCConstants.VARCHARH);
				tableDescription.put("event_type", DCCConstants.VARCHARH);
				tableDescription.put("event_datetime", DCCConstants.TIMESTAMPZ);
				tableDescription.put("event_payload", DCCConstants.TEXT);
				tableDescription.put("poi_event_payload", DCCConstants.TEXT);
				tableDescription.put("event_operation", DCCConstants.VARCHARH);
				tableDescription.put("retry_attempts", DCCConstants.LONGINTEGER);
				tableDescription.put("job_set_time", DCCConstants.TIMESTAMP);

			} else if (tableName.equalsIgnoreCase("dcc_job_queue_arch")) {
				tableDescription.put("event_id", DCCConstants.VARCHARH);
				tableDescription.put("event_type", DCCConstants.VARCHARH);
				tableDescription.put("event_datetime", DCCConstants.TIMESTAMPZ);
				tableDescription.put("event_payload", DCCConstants.TEXT);
				tableDescription.put("poi_event_payload", DCCConstants.TEXT);
				tableDescription.put("event_operation", DCCConstants.VARCHARH);
				tableDescription.put("retry_attempts", DCCConstants.LONGINTEGER);
				tableDescription.put("job_set_time", DCCConstants.TIMESTAMP);

			} else if (tableName.equalsIgnoreCase("dcc_audit")) {
				tableDescription.put("audit_id", DCCConstants.VARCHARH);
				tableDescription.put("event_id", DCCConstants.VARCHARH);
				tableDescription.put("event_operation", DCCConstants.VARCHARH);
				tableDescription.put("activity_start_datetime", DCCConstants.TIMESTAMP);
				tableDescription.put("activity_end_datetime", DCCConstants.TIMESTAMP);
				tableDescription.put("audit_datetime", DCCConstants.TIMESTAMP);
				tableDescription.put("exception", DCCConstants.TEXT);

			} else if (tableName.equalsIgnoreCase("dcc_ord_rist_items")) {
				tableDescription.put("order_id", DCCConstants.VARCHARH);
				tableDescription.put("customer_id", DCCConstants.VARCHARH);
				tableDescription.put("fulfilment_id", DCCConstants.VARCHARH);
				tableDescription.put("item_id", DCCConstants.TEXT);
				tableDescription.put("is_liq", DCCConstants.VARCHARH);
				tableDescription.put("store_id", DCCConstants.VARCHARH);
				tableDescription.put("delivery_datetime", DCCConstants.TIMESTAMP);
				tableDescription.put("created_datetime", DCCConstants.TIMESTAMP);

			} else if (tableName.equalsIgnoreCase("dcc_customer_details")) {

				tableDescription.put("customer_id", DCCConstants.VARCHARH);
				tableDescription.put("liq_flag", DCCConstants.BOOLEAN);
				tableDescription.put("order_id", DCCConstants.TEXT);
				tableDescription.put("store_id", DCCConstants.VARCHARH);
				tableDescription.put("last_update_dateime", DCCConstants.TIMESTAMP);

			} else if (tableName.equalsIgnoreCase("dcc_event_consumption_stats")) {

				tableDescription.put("event_id", DCCConstants.VARCHARH);
				tableDescription.put("event_type", DCCConstants.VARCHARH);
				tableDescription.put("event_sequence_num", DCCConstants.VARCHARF);
				tableDescription.put("partition_id", "varchar,5");
				tableDescription.put("last_enq_event_sequence_num", DCCConstants.VARCHARF);
				tableDescription.put("node_id", DCCConstants.VARCHARH);
				tableDescription.put("created_datetime", DCCConstants.TIMESTAMP);

			} else if (tableName.equalsIgnoreCase("dcc_active_queue")) {
				tableDescription.put("event_id", DCCConstants.VARCHARH);
				tableDescription.put("created_datetimestamp", DCCConstants.TIMESTAMPZ);

			}
			boolean validTable = DatabaseColumnValidationUtil.checkDBConnection(tableName.toLowerCase(),
					tableDescription);

			if (validTable) {
				logs.info("Database Schema Validation is done successfully");
			} else {
				logs.info("Database Schema Validation is not done successfully");
			}
			assertTrue("Database Schema Validation is not done successfully for " + tableName, validTable);
		} catch (Exception e) {
			fail(e.getMessage());
		} finally {
			logs.info("Complete");
		}
	}

	@Then("Validate the {string} should be present in following tables")
	public void validate_the_should_be_present_in_following_tables(String event_id,
			io.cucumber.datatable.DataTable dataTable) throws Exception {
		List<String> tables = dataTable.asList();

		for (String tableName : tables) {
			Boolean isPersist = DatabaseColumnValidationUtil.iseventpersist(event_id, tableName);
			assertTrue("the event is not persisted in the " + tableName, isPersist);
		}
	}

	@Then("Validate {string} should persist into dcc_ord_rist_items table or not")
	public void validate_should_persist_into_dcc_ord_rist_items_table_or_not(String order_id) throws Exception {
		// Write code here that turns the phrase above into concrete actions
		Map<String, String> DBData = DatabaseColumnValidationUtil.getordlistRecord(order_id);
		assertNotNull("the event is not persisted in the dcc_ord_rist_items", DBData.get("order_id"));
	}

	@Then("Validate the {string} should not present in following tables")
	public void validate_the_should_not_present_in_following_tables(String event_id,
			io.cucumber.datatable.DataTable dataTable) throws Exception {
		List<String> tables = dataTable.asList();

		for (String tableName : tables) {
			Boolean isPersist = DatabaseColumnValidationUtil.iseventpersist(event_id, tableName);
			assertFalse("the event is persisted in the " + tableName, isPersist);
		}
	}

	@Given("Wait for {string} until MFSev receives response")
	public void wait_for_until_mf_sev_receives_response(String time) {
		try {
			Thread.sleep(Long.valueOf(time) * 40);
		} catch (InterruptedException e) {

			Thread.currentThread().interrupt();
		}
	}

	@Given("Validate the dcc_job_queue_arch table column for the following columns with {string}")
	public void validate_the_dcc_job_queue_arch_table_column_for_the_following_columns_with(String event_id,
			io.cucumber.datatable.DataTable dataTable) throws Exception {
		List<Map<String, String>> expectedDataList = dataTable.asMaps(String.class, String.class);
		Map<String, String> databaseData = DatabaseColumnValidationUtil.getdccjobqueuearchRecord(event_id);
		System.out.println(databaseData);
		System.out.println(expectedDataList);
		for (Map<String, String> expectedData : expectedDataList) {
			for (String key : expectedData.keySet()) {
				if (expectedData.get(key).equalsIgnoreCase("null")) {
					assertNull("fa in the column: " + key, databaseData.get(key));
				} else {
					assertEquals("DataMisMatch in the column: " + key, expectedData.get(key), databaseData.get(key));
				}
			}
		}
	}

	@Given("Validate the dcc_ord_rist_items table column for the following columns with {string}")
	public void validate_the_dcc_ord_rist_items_table_column_for_the_following_columns_with(String order_id,
			io.cucumber.datatable.DataTable dataTable) throws Exception {
		List<Map<String, String>> expectedDataList = dataTable.asMaps(String.class, String.class);
		Map<String, String> databaseData = DatabaseColumnValidationUtil.getordlistRecord(order_id);
		System.out.println(databaseData);
		System.out.println(expectedDataList);
		for (Map<String, String> expectedData : expectedDataList) {
			for (String key : expectedData.keySet()) {
				if (expectedData.get(key).equalsIgnoreCase("null")) {
					assertNull("fa in the column: " + key, databaseData.get(key));
				} else {
					assertEquals("DataMisMatch in the column: " + key, expectedData.get(key), databaseData.get(key));
				}
			}
		}
	}

	@Then("Valiate the following operation should persist in the ofeh_audit table or not for the {string}")
	public void valiate_the_following_operation_should_persist_in_the_ofeh_audit_table_or_not_for_the(String event_id,
			io.cucumber.datatable.DataTable dataTable) throws SQLException, IOException {
		List<String> tables = dataTable.asList();

		for (String OPERATION : tables) {
			Boolean isPersist = DatabaseColumnValidationUtil.getdccauditOps(event_id, OPERATION);
			assertTrue("Event id " + event_id + " and event_operation " + OPERATION + " is not persited in DCC_Audit",
					isPersist);
			logs.info("Event id " + event_id + " and event_operation " + OPERATION + " is persited in DCC_Audit");
		}
	}

	@Given("Validate the {string} dcc_job_queue_arch table column for the following columns with {string}")
	public void validate_the_dcc_job_queue_arch_table_column_for_the_following_columns_with(String FileName,
			String event_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getdccjobqueuearchRecord(event_id);
		assertNotNull("POI payload column is null", jobQueueMap.get("poi_event_payload"));
		String POIResponse = jobQueueMap.get("poi_event_payload");
		logs.info(POIResponse);
		JSONParser parser = new JSONParser();
		JSONArray jsonArr = (JSONArray) parser.parse(POIResponse);
		JSONObject json = (JSONObject) jsonArr.get(0);

		String id = (String) json.get("id");
		String time = (String) json.get("time");
		JSONObject jsonData = (JSONObject) json.get("data");
		String updatedDateTime = (String) jsonData.get("updatedDateTime");
		String aId = (String) jsonData.get("associationId");

		String json1 = POIResponse;
		json1 = json1.replace(id.toString(), "1007");
		json1 = json1.replace(time.toString(), "01-01-2025");
		json1 = json1.replace(updatedDateTime, "01-01-2025");
		json1 = json1.replace(aId, "20251031113719");

		System.out.println(json1);

		Path fileName = Path.of("src/main/resources/POIReponseFiles/" + FileName + ".json");
		String payload = Files.readString(fileName);
		logs.info(payload);

		assertEquals("Data validation is not done successfully", payload, json1);
		logs.info("Fulfilment_order data validation is done successfully");
	}

	@Given("Validate the poi_payload in dcc_job_queue_arch table for {string} should be null")
	public void validate_the_poi_payload_in_dcc_job_queue_arch_table_for_should_be_null(String event_id)
			throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getdccjobqueuearchRecord(event_id);
		assertNull("POI payload column is not null in the DCC_JOB_QUEUE_ARCH", jobQueueMap.get("poi_event_payload"));
	}

	@Then("Validate the {string} should not present in the dcc_ord_rist_items table")
	public void validate_the_should_not_present_in_the_dcc_ord_rist_items_table(String order_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getordlistRecord(order_id);
		assertNull("Order details is persisted in the DCC_ORD_RIST_ITEMS", jobQueueMap.get("order_id"));
	}

	@Then("Validate the {string} should present in the dcc_ord_rist_items table")
	public void validate_the_should_present_in_the_dcc_ord_rist_items_table(String order_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getordlistRecord(order_id);
		assertNotNull("Order details is not persisted in the DCC_ORD_RIST_ITEMS", jobQueueMap.get("order_id"));
	}

	@Then("Validate the dcc_event_handled table should have single entry for the {string}")
	public void validate_the_dcc_event_handled_table_should_have_single_entry_for_the(String event_id)
			throws Exception {
		assertTrue("There is no entry for the event id in DCC EVENT HANDLED table for " + event_id
				+ "in DCC EVENT HANDLED table", DatabaseColumnValidationUtil.recordcountindcchandled(event_id));
	}

	@Then("Validate the dcc_job_queue_arch table should have single entry for the {string}")
	public void validate_the_dcc_job_queue_arch_table_should_have_single_entry_for_the(String event_id)
			throws Exception {
		assertTrue(
				"There is no entry for the event id in DCC JOB QUEUE ARCH table for " + event_id
						+ "in DCC JOB QUEUE ARCH table",
				DatabaseColumnValidationUtil.recordcountindccqueuearch(event_id));
	}

	@Then("Validate Entry to be made in dcc_audit table for {string}")
	public void validate_entry_to_be_made_in_dcc_audit_table_for(String event_id) throws Exception {
		assertTrue("There is no entry for the event id in DCC Audit table for " + event_id + "in DCC Audit table",
				DatabaseColumnValidationUtil.recordcountindccaudit(event_id));
	}

	@Then("Validate the Retry_Attempt column of dcc_job_queue table for the {string} should be {string}")
	public void validate_the_retry_attempt_column_of_dcc_job_queue_table_for_the_should_be(String event_id,
			String retryAttemps) throws Exception {

		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getdccjobqueueRecord(event_id);
		assertEquals("The retry attempts is not valid for the event-id : " + event_id + " in the dcc_job_queue table",
				retryAttemps, jobQueueMap.get("retry_attempts"));
	}

	@Then("Validate the {string} should persist in the dcc_job_queue table")
	public void validate_the_should_persist_in_the_dcc_job_queue_table(String event_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getdccjobqueueRecord(event_id);
		assertEquals("The event ID is not for valid in the dcc_job_queue table", event_id, jobQueueMap.get("event_id"));

	}

	@Then("Validate the {string} should not persist in the dcc_job_queue_arch table")
	public void validate_the_should_not_persist_in_the_dcc_job_queue_arch_table(String event_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getdccjobqueuearchRecord(event_id);
		assertNull("The event ID is not null in the dcc_job_queue_arch table", jobQueueMap.get("event_id"));
	}

	@Then("Validate the Exception to be Persisted in dcc_audit table for {string}")
	public void validate_the_exception_to_be_persisted_in_dcc_audit_table_for(String event_id) throws Exception {
		assertNotNull("The Exception is not persisted in DCC_AUDIT table for the " + event_id,
				DatabaseColumnValidationUtil.exceptionindccaudit(event_id));
	}

	@Then("Validate the event operation column of dcc_job_queue table should be {string} at the end of the flow for {string}")
	public void validate_the_event_operation_column_of_dcc_job_queue_table_should_be_at_the_end_of_the_flow_for(
			String operation, String event_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getdccjobqueueRecord(event_id);
		assertEquals("The event operation is not for valid in the dcc_job_queue table for the event_id :" + event_id,
				operation, jobQueueMap.get("event_operation"));
	}

	@Then("Validate the {string} should persist in the dcc_active_queue table")
	public void validate_the_event_should_persist_in_the_dcc_active_queue_table(String event_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getactivequeuerecord(event_id);
		assertNotNull("The event ID is persisted in the dcc_active_queue table", jobQueueMap.get("event_id"));
	}

	@Given("insert the record in customer_details table for the {string}")
	public void insert_the_record_in_customer_details_table_for_the(String customer_id) throws Exception {
		assertTrue("Failed during inserting record in dcc_customer_details table",
				DatabaseColumnValidationUtil.insertcustdetailsrecord(customer_id));
	}

	@Then("Validate the {string} should present in the dcc_customer_details table")
	public void validate_the_should_present_in_the_dcc_customer_details_table(String customer_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getcustdetailsrecord(customer_id);
		assertNotNull("The Customer ID is not persisted in the dcc_customer_details table",
				jobQueueMap.get("customer_id"));
	}

	@Then("Validate the {string} should not present in the dcc_customer_details table")
	public void validate_the_should_not_present_in_the_dcc_customer_details_table(String customer_id) throws Exception {
		Map<String, String> jobQueueMap = DatabaseColumnValidationUtil.getcustdetailsrecord(customer_id);
		assertNull("The Customer ID is persisted in the dcc_customer_details table", jobQueueMap.get("customer_id"));

	}

	
	
	// Started
	   
		// declaring the paylaod , expected file path , schema file path
	    @Given("payload file {string} and expected file {string} and schema dir {string}")
	    public void setup(String payload, String expected, String schemaDir) throws Exception {	       
	        this.payloadPath = payload;
	        this.expectedPath = expected;
	        this.schemaDir = schemaDir;
	        this.payloadArray = mapper.readTree(Path.of(payload).toFile());
	    }

	    // works on event trigger
	    @When("event is triggered")
	    public void triggerEvent() throws Exception {
	        // Event triggering intentionally skipped; validation reads IDs from payload file directly.
	    }

	    @Then("select event-id {string} and order-id {string}")
	    public void selectEvent(String eventId, String orderId) {
	        this.selectedEventId = eventId;
	        this.selectedOrderId = orderId;
	    }
	    
	    // Validation
	    @Then("database values should match expected data")
	    public void validateDatabase() throws Exception {
	    	
	        if (!payloadArray.isArray()) {
	            throw new IllegalArgumentException("Payload file must be a JSON array");
	        }
	        Set<String> payloadEventIds = new HashSet<>();
	        Set<String> payloadOrderIds = new HashSet<>();
	        for (JsonNode payloadNode2 : payloadArray) {
	        	try {

//	        		assertTrue("Event is not triggered..", EventTriggerUtil.eventTriggerPayload(payloadNode2));
	        		for (JsonNode payloadNode : payloadNode2) {
	    			eventType = payloadNode.path("type").asText();
	    			String tempid=  payloadNode.path("id").asText();
	    			String temporderid=payloadNode.path("orderId").asText();
	    			payloadEventIds.add(payloadNode.path("id").asText());
		            payloadOrderIds.add(payloadNode.path("orderId").asText()); 
		            logs.info("Event-id : "+tempid + " OrderID :" + temporderid+" Event is triggered");
	    		}
	        	}
	        	catch (Exception e) {
	    			fail(e.getMessage());
	    			e.printStackTrace();

	    		}
	         	        

	        // throw error if the event idd and order id not found in the payload
	        for (JsonNode payloadNode : payloadNode2) {
	            String eventId = payloadNode.path("id").asText();
	            String orderId = payloadNode.path("data").path("orderId").asText();
	            if (selectedEventId != null && !selectedEventId.isEmpty()) {
	                if (!selectedEventId.equals(eventId) || !selectedOrderId.equals(orderId)) {
	                    continue;
	                }
	            }
	            System.out.println("[Scenario - event_id - " + eventId + " and order-id " + orderId + " ] =>");
	            logScenario("Scenario event-id=" + eventId + " order-id=" + orderId);

	            

	            List<ExpectedTable> tables = loadExpectedTables(payloadEventIds, payloadOrderIds);
	            if (tables.isEmpty()) {
	                throw new IllegalStateException("No expected files found in expected directory. Validation cannot proceed.");
	            }

	            List<String> dbTables = DatabaseColumnValidationUtil.listTables();
	            List<String> missingExpected = new ArrayList<>();
	            //checking all expected files are there are not 
	            for (String t : dbTables) {
//	            	System.out.println(Path.of(Path.of(expectedPath).getParent().toString()));
	                Path expectedFile = Path.of(Path.of(expectedPath).getParent().toString(), t + "_expected_data.json");
	                if (!java.nio.file.Files.exists(expectedFile)) {
	                    missingExpected.add(t);
	                }
	            }
	            if (!missingExpected.isEmpty()) {
	                throw new IllegalStateException("Expected file missing for tables: " + String.join(", ", missingExpected));
	            }

	            for (ExpectedTable table : tables) {
	            	//fetching expected row by event_id or order id
	                JsonNode expectedRow = table.expectedById.containsKey(eventId) ? table.expectedById.get(eventId) : table.expectedByOrder.get(orderId);
 	                if (expectedRow == null) {
 	                	 System.out.println(" Event details for "+eventId+" TableName not found in expected data- " + table.tableName);
	                    continue;
	                }
	                System.out.println("TableName - " + table.tableName);
	                logScenario("Table: " + table.tableName);

	                LookupConfig lookup = resolveLookup(table.tableName, expectedRow);
	                List<Map<String, Object>> actualRows;
	                try {
	                    actualRows = DatabaseColumnValidationUtil.fetchByLookup(	      
	                            table.tableName,
	                            lookup.idColumn,
	                            eventId,
	                            lookup.orderIdColumn,
	                            orderId
	                    );
	                } catch (SQLException ex) {
	                    throw new RuntimeException("DB fetch failed for table " + table.tableName, ex);
	                }
	                
	                if (expectedRow != null && actualRows.isEmpty()) {
	                	 System.out.println("TableName - " + table.tableName+" is not found in DB Expected data but not found");
	            
	                    continue;
	                }
	                
	                
	                // log("DB: rows fetched=" + actualRows.size());

	                enrichSchemaWithColumnRules(table.tableName, table.schema, expectedRow);
	                JsonNode expectedArray = mapper.createArrayNode().add(expectedRow);
	                ValidationReport report = jsonCompare.validateTable(eventId, table.tableName, actualRows, expectedArray, table.schema);
	                reports.add(report);
	                printScenarioTableSummary(report);
	            }
	        }
	        }
	        

	        // String reportJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(reports);
	        // System.out.println(reportJson);

	        for (ValidationReport r : reports) {
	            if ("FAIL".equals(r.status)) {
	                // defer assertion to print consolidated failure summary
	            }
	        }

	        printFailedSummary(reports);
	        Allure.addAttachment("validation-log", "text/plain", scenarioLog.toString());

	        for (ValidationReport r : reports) {
	            if ("FAIL".equals(r.status)) {
	                throw new AssertionError("Validation failed for table " + r.tableName);
	            }
	        }
	    }

	    // logs suppressed

	   private void enrichSchemaWithColumnRules(String tableName, JsonCompare.Schema schema, JsonNode expectedRow) throws Exception {
	    Iterator<String> fields = expectedRow.fieldNames();
	    while (fields.hasNext()) {
	        String column = fields.next();
	        JsonNode value = expectedRow.get(column);

	        // Only try schema for JSON-like columns
	        boolean looksJson = value.isObject() || value.isArray() ||
	                (value.isTextual() && (value.asText().trim().startsWith("{") || value.asText().trim().startsWith("[")));
	        if (!looksJson) {
	            continue;
	        }

	        String fileName = tableName + "_" + column + ".schema.json";
	        Path path = Path.of(schemaDir, fileName);
	        if (java.nio.file.Files.exists(path)) {
	            JsonCompare.ColumnRule rule = columnRuleCache.get(path.toString());
	            if (rule == null) {
	                rule = jsonCompare.loadColumnRule(path);
	                columnRuleCache.put(path.toString(), rule);
	            }
	            schema.rules.put(column, rule);
	            // log("SCHEMA: loaded column schema " + fileName);
	        } else {
	            // log("SCHEMA: column schema not found for " + column + " (expected " + fileName + ")");
	        }
	    }
	}

	    private List<ExpectedTable> loadExpectedTables(Set<String> payloadEventIds, Set<String> payloadOrderIds) throws Exception {
	        List<ExpectedTable> tables = new ArrayList<>();
	        Path expectedDir = Path.of(expectedPath).getParent();
	        if (expectedDir == null) {
	            throw new IllegalArgumentException("Expected directory not found for " + expectedPath);
	        }
	        try (java.util.stream.Stream<java.nio.file.Path> stream = java.nio.file.Files.list(expectedDir)) {
	            List<Path> files = stream
	                    .filter(p -> p.getFileName().toString().endsWith("_expected_data.json"))
	                    .toList();
	            for (Path p : files) {
	                String file = p.getFileName().toString();
	                String tableName = file.substring(0, file.indexOf("_expected_data.json"));
	                JsonNode rows = jsonCompare.loadExpected(p);
	                if (!rows.isArray()) {
	                    continue;
	                }
	                Map<String, JsonNode> expectedById = new HashMap<>();
	                Map<String, JsonNode> expectedByOrder = new HashMap<>();
	                for (JsonNode row : rows) {
	                    String rowId = row.has("event_id") ? row.get("event_id").asText() : "";
	                    String rowOrderId = row.has("order_id") ? row.get("order_id").asText() : "";
	                    if (payloadEventIds.contains(rowId) || payloadOrderIds.contains(rowOrderId)) {
	                        if (!rowId.isEmpty()) expectedById.put(rowId, row);
	                        if (!rowOrderId.isEmpty()) expectedByOrder.put(rowOrderId, row);
	                    }
	                }
	                JsonCompare.Schema schema = new JsonCompare.Schema();
	                schema.tableName = tableName;
	                tables.add(new ExpectedTable(tableName, expectedById, expectedByOrder, schema));
	                // logs suppressed
	            }
	        }
	        return tables;
	    }

	    private LookupConfig resolveLookup(String tableName, JsonNode expectedRow) throws Exception {
	        LookupConfig cfg = new LookupConfig();
	        schemaDir="src/main/resources/lookups/dcc_job_queue_arch_lookup.json";
	        Path lookupFile = Path.of(schemaDir, tableName + "_lookup.json");
	        if (java.nio.file.Files.exists(lookupFile)) {
	            JsonNode node = mapper.readTree(lookupFile.toFile());
	            cfg.idColumn = node.has("idColumn") ? node.get("idColumn").asText() : null;
	            cfg.orderIdColumn = node.has("orderIdColumn") ? node.get("orderIdColumn").asText() : null;
	            return cfg;
	        }
	        if (expectedRow.has("id")) cfg.idColumn = "event_id";
	        if (expectedRow.has("orderid")) cfg.orderIdColumn = "order_id";
	        if (expectedRow.has("event_id")) cfg.idColumn = "event_id";
	        if (expectedRow.has("order_id")) cfg.orderIdColumn = "order_id";
	        return cfg;
	    }

	    private static class LookupConfig {
	        String idColumn;
	        String orderIdColumn;
	    }

	    private static class ExpectedTable {
	        final String tableName;
	        final Map<String, JsonNode> expectedById;
	        final Map<String, JsonNode> expectedByOrder;
	        final JsonCompare.Schema schema;

	        ExpectedTable(String tableName, Map<String, JsonNode> expectedById, Map<String, JsonNode> expectedByOrder, JsonCompare.Schema schema) {
	            this.tableName = tableName;
	            this.expectedById = expectedById;
	            this.expectedByOrder = expectedByOrder;
	            this.schema = schema;
	        }
	    }

	    private String writeTempPayload(JsonNode payloadNode) throws Exception {
	        Path temp = Path.of("target", "payload-" + UUID.randomUUID() + ".json");
	        java.nio.file.Files.createDirectories(temp.getParent());
	        java.nio.file.Files.writeString(temp, mapper.writeValueAsString(payloadNode));
	        return temp.toString();
	    }

	    private void printScenarioTableSummary(ValidationReport report) {
	    for (JsonCompare.ColumnResult r : report.results) {
	        if ("PASS".equals(r.status)) {
	            System.out.println(r.column + " -> PASS");
	            logScenario(r.column + " -> PASS");
	        } else {
	                System.out.println(r.column + " -> " + r.status +
	                    " | expected=" + clean(r.expected) + " | actual=" + clean(r.actual));
	                logScenario(r.column + " -> " + r.status +
	                        " | expected=" + clean(r.expected) + " | actual=" + clean(r.actual));
	        }
	    }
	}

	    private void printFailedSummary(List<ValidationReport> reports) {
	        boolean anyFail = false;
	        for (ValidationReport r : reports) {
	            if ("FAIL".equals(r.status)) {
	                anyFail = true;
	                break;
	            }
	        }
	        if (!anyFail) {
	            return;
	        }
	        System.out.println("Failed test cases and their respective logs");
	        for (ValidationReport report : reports) {
	            if (!"FAIL".equals(report.status)) {
	                continue;
	            }
	            System.out.println("Table: " + report.tableName + " | EventId: " + report.eventId);
	            for (JsonCompare.ColumnResult r : report.results) {
	                if ("FAIL".equals(r.status) || "SKIPPED".equals(r.status)) {
	                    System.out.println("  " + r.column + " -> " + r.status +
	                            " | expected=" + clean(r.expected) + " | actual=" + clean(r.actual) + " | reason=" + r.reason);
	                    logScenario("  " + r.column + " -> " + r.status +
	                            " | expected=" + clean(r.expected) + " | actual=" + clean(r.actual) + " | reason=" + r.reason);
	                }
	            }
	        }
	    }

	    private String clean(String s) {
	        if (s == null) return null;
	        return s.replace("\r", "")
	                .replace("\n", "")
	                .replace("\t", "")
	                .trim();
	    }

	    private void logScenario(String msg) {
	        scenarioLog.append(msg).append(System.lineSeparator());
	    }

	 
}
