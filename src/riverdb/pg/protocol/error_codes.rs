// Known Postgres error codes
// Class 00 — Successful Completion
pub const SUCCESSFUL_COMPLETION: &str = "00000"; // successful_completion
// Class 01 — Warning
pub const WARNING: &str = "01000"; // warning
pub const WARNING_DYNAMIC_RESULT_SETS_RETURNED: &str = "0100C"; // dynamic_result_sets_returned
pub const WARNING_IMPLICIT_ZERO_BIT_PADDING: &str = "01008"; // implicit_zero_bit_padding
pub const WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION: &str = "01003"; // null_value_eliminated_in_set_function
pub const WARNING_PRIVILEGE_NOT_GRANTED: &str = "01007"; // privilege_not_granted
pub const WARNING_PRIVILEGE_NOT_REVOKED: &str = "01006"; // privilege_not_revoked
pub const WARNING_STRING_DATA_RIGHT_TRUNCATION: &str = "01004"; // string_data_right_truncation
pub const WARNING_DEPRECATED_FEATURE: &str = "01P01"; // deprecated_feature
// Class 02 — No Data (this is also a warning class per the SQL standard)
pub const NO_DATA: &str = "02000"; // no_data
pub const NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED: &str = "02001"; // no_additional_dynamic_result_sets_returned
// Class 03 — SQL Statement Not Yet Complete
pub const SQL_STATEMENT_NOT_YET_COMPLETE: &str = "03000"; // sql_statement_not_yet_complete
// Class 08 — Connection Exception
pub const CONNECTION_EXCEPTION: &str = "08000"; // connection_exception
pub const CONNECTION_DOES_NOT_EXIST: &str = "08003"; // connection_does_not_exist
pub const CONNECTION_FAILURE: &str = "08006"; // connection_failure
pub const SQL_CLIENT_UNABLE_TO_ESTABLISH_SQL_CONNECTION: &str = "08001"; // sqlclient_unable_to_establish_sqlconnection
pub const SQL_SERVER_REJECTED_ESTABLISHEMENT_OF_SQL_CONNECTION: &str = "08004"; // sqlserver_rejected_establishment_of_sqlconnection
pub const TRANSACTION_RESOLUTION_UNKNOWN: &str = "08007"; // transaction_resolution_unknown
pub const PROTOCOL_VIOLATION: &str = "08P01"; // protocol_violation
// Class 09 — Triggered Action Exception
pub const TRIGGERED_ACTION_EXCEPTION: &str = "09000"; // triggered_action_exception
// Class 0A — Feature Not Supported
pub const FEATURE_NOT_SUPPORTED: &str = "0A000"; // feature_not_supported
// Class 0B — Invalid Transaction Initiation
pub const INVALID_TRANSACTION_INITIATION: &str = "0B000"; // invalid_transaction_initiation
// Class 0F — Locator Exception
pub const LOCATOR_EXCEPTION: &str = "0F000"; // locator_exception
pub const INVALID_LOCATOR_SPECIFICATION: &str = "0F001"; // invalid_locator_specification
// Class 0L — Invalid Grantor
pub const INVALID_GRANTOR: &str = "0L000"; // invalid_grantor
pub const INVALID_GRANT_OPERATION: &str = "0LP01"; // invalid_grant_operation
// Class 0P — Invalid Role Specification
pub const INVALID_ROLE_SPECIFICATION: &str = "0P000"; // invalid_role_specification
// Class 0Z — Diagnostics Exception
pub const DIAGNOSTICS_EXCEPTION: &str = "0Z000"; // diagnostics_exception
pub const STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER: &str = "0Z002"; // stacked_diagnostics_accessed_without_active_handler
// Class 20 — Case Not Found
pub const CASE_NOT_FOUND: &str = "20000"; // case_not_found
// Class 21 — Cardinality Violation
pub const CARDINALITY_VIOLATION: &str = "21000"; // cardinality_violation
// Class 22 — Data Exception
pub const DATA_EXCEPTION: &str = "22000"; // data_exception
pub const ARRAY_SUBSCRIPT_ERROR: &str = "2202E"; // array_subscript_error
pub const CHARACTER_NOT_IN_REPERTOIRE: &str = "22021"; // character_not_in_repertoire
pub const DATETIME_FIELD_OVERFLOW: &str = "22008"; // datetime_field_overflow
pub const DIVISION_BY_ZERO: &str = "22012"; // division_by_zero
pub const ERROR_IN_ASSIGNMENT: &str = "22005"; // error_in_assignment
pub const ESCAPE_CHARACTER_CONFLICT: &str = "2200B"; // escape_character_conflict
pub const INDICATOR_OVERFLOW: &str = "22022"; // indicator_overflow
pub const INTERVAL_FIELD_OVERFLOW: &str = "22015"; // interval_field_overflow
pub const INVALID_ARGUMENT_FOR_LOGARITHM: &str = "2201E"; // invalid_argument_for_logarithm
pub const INVALID_ARGUMENT_FOR_N_TILE_FUNCTION: &str = "22014"; // invalid_argument_for_ntile_function
pub const INVALID_ARGUMENT_FOR_NTH_VALUE_FUNCTION: &str = "22016"; // invalid_argument_for_nth_value_function
pub const INVALID_ARGUMENT_FOR_POWER_FUNCTION: &str = "2201F"; // invalid_argument_for_power_function
pub const INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION: &str = "2201G"; // invalid_argument_for_width_bucket_function
pub const INVALID_CHARACTER_VALUE_FOR_CAST: &str = "22018"; // invalid_character_value_for_cast
pub const INVALID_DATETIME_FORMAT: &str = "22007"; // invalid_datetime_format
pub const INVALID_ESCAPE_CHARACTER: &str = "22019"; // invalid_escape_character
pub const INVALID_ESCAPE_OCTET: &str = "2200D"; // invalid_escape_octet
pub const INVALID_ESCAPE_SEQUENCE: &str = "22025"; // invalid_escape_sequence
pub const NON_STANDARD_USE_OF_ESCAPE_CHARACTER: &str = "22P06"; // nonstandard_use_of_escape_character
pub const ERROR_CODE_INVALID_INDICATOR_PARAMETER_VALUE: &str = "22010"; // invalid_indicator_parameter_value
pub const INVALID_PARAMETER_VALUE: &str = "22023"; // invalid_parameter_value
pub const INVALID_REGULAR_EXPRESSION: &str = "2201B"; // invalid_regular_expression
pub const INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: &str = "2201W"; // invalid_row_count_in_limit_clause
pub const INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: &str = "2201X"; // invalid_row_count_in_result_offset_clause
pub const INVALID_TABLESAMPLE_ARGUMENT: &str = "2202H"; // invalid_tablesample_argument
pub const INVALID_TABLESAMPLE_REPEAT: &str = "2202G"; // invalid_tablesample_repeat
pub const INVALID_TIME_ZONE_DISPLACEMENT_VALUE: &str = "22009"; // invalid_time_zone_displacement_value
pub const INVALID_INVALID_USE_OF_ESCAPE_CHARACTER: &str = "2200C"; // invalid_use_of_escape_character
pub const MOST_SPECIFIC_TYPE_MISMATCH: &str = "2200G"; // most_specific_type_mismatch
pub const NULL_VALUE_NOT_ALLOWED: &str = "22004"; // null_value_not_allowed
pub const NULL_VALUE_NO_INDICATOR_PARAMETER: &str = "22002"; // null_value_no_indicator_parameter
pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003"; // numeric_value_out_of_range
pub const STRING_DATA_LENGTH_MISMATCH: &str = "22026"; // string_data_length_mismatch
pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001"; // string_data_right_truncation
pub const SUBSTRING_ERROR: &str = "22011"; // substring_error
pub const TRIM_ERROR: &str = "22027"; // trim_error
pub const UNTERMINATED_C_STRING: &str = "22024"; // unterminated_c_string
pub const ZERO_LENGTH_CHARACTER_STRING: &str = "2200F"; // zero_length_character_string
pub const FLOATING_POINT_EXCEPTION: &str = "22P01"; // floating_point_exception
pub const INVALID_TEXT_REPRESENTATION: &str = "22P02"; // invalid_text_representation
pub const INVALID_BINARY_REPRESENTATION: &str = "22P03"; // invalid_binary_representation
pub const BAD_COPY_FILE_FORMAT: &str = "22P04"; // bad_copy_file_format
pub const UNTRANSLATABLE_CHARACTER: &str = "22P05"; // untranslatable_character
pub const NOT_AN_XML_DOCUMENT: &str = "2200L"; // not_an_xml_document
pub const INVALID_XML_DOCUMENT: &str = "2200M"; // invalid_xml_document
pub const INVALID_XML_CONTENT: &str = "2200N"; // invalid_xml_content
pub const INVALID_XML_COMMENT: &str = "2200S"; // invalid_xml_comment
pub const INVALID_XML_PROCESSING_INSTRUCTION: &str = "2200T"; // invalid_xml_processing_instruction
// // Class 23 — Integrity Constraint Violation
pub const INTEGRITY_CONSTRAINT_VIOLATION: &str = "23000"; // integrity_constraint_violation
pub const RESTRICT_VIOLATION: &str = "23001"; // restrict_violation
pub const NOT_NULL_VIOLATION: &str = "23502"; // not_null_violation
pub const FOREIGN_KEY_VIOLATION: &str = "23503"; // foreign_key_violation
pub const UNIQUE_VIOLATION: &str = "23505"; // unique_violation
pub const CHECK_VIOLATION: &str = "23514"; // check_violation
pub const EXCLUSION_VIOLATION: &str = "23P01"; // exclusion_violation
// // Class 24 — Invalid Cursor State
pub const INVALID_CURSOR_STATE: &str = "24000"; // invalid_cursor_state
// // Class 25 — Invalid Transaction State
pub const INVALID_TRANSACTION_STATE: &str = "25000"; // invalid_transaction_state
pub const ACTIVE_SQL_TRANSACTION: &str = "25001"; // active_sql_transaction
pub const BRANCH_TRANSACTION_ALREADY_ACTIVE: &str = "25002"; // branch_transaction_already_active
pub const HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL: &str = "25008"; // held_cursor_requires_same_isolation_level
pub const INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION: &str = "25003"; // inappropriate_access_mode_for_branch_transaction
pub const INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION: &str = "25004"; // inappropriate_isolation_level_for_branch_transaction
pub const NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION: &str = "25005"; // no_active_sql_transaction_for_branch_transaction
pub const READ_ONLY_SQL_TRANSACTION: &str = "25006"; // read_only_sql_transaction
pub const SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED: &str = "25007"; // schema_and_data_statement_mixing_not_supported
pub const NO_ACTIVE_SQL_TRANSACTION: &str = "25P01"; // no_active_sql_transaction
pub const IN_FAILED_SQL_TRANSACTION: &str = "25P02"; // in_failed_sql_transaction
pub const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: &str = "25P03"; // idle_in_transaction_session_timeout
// Class 26 — Invalid SQL Statement Name
pub const INVALID_SQL_STATEMENT_NAME: &str = "26000"; // invalid_sql_statement_name
// Class 27 — Triggered Data Change Violation
pub const TRIGGERED_DATA_CHANGE_VIOLATION: &str = "27000"; // triggered_data_change_violation
// Class 28 — Invalid Authorization Specification
pub const INVALID_AUTHORIZATION_SPECIFICATION: &str = "28000"; // invalid_authorization_specification
pub const INVALID_PASSWORD: &str = "28P01"; // invalid_password
// Class 2B — Dependent Privilege Descriptors Still Exist
pub const DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST: &str = "2B000"; // dependent_privilege_descriptors_still_exist
pub const DEPENDENT_OBJECTS_STILL_EXIST: &str = "2BP01"; // dependent_objects_still_exist
// Class 2D — Invalid Transaction Termination
pub const INVALID_TRANSACTION_TERMINATION: &str = "2D000"; // invalid_transaction_termination
// Class 2F — SQL Routine Exception
pub const ROUTINE_SQL_RUNTIME_EXCEPTION: &str = "2F000"; // sql_routine_exception
pub const ROUTINE_FUNCTION_EXECUTED_NO_RETURN_STATEMENT: &str = "2F005"; // function_executed_no_return_statement
pub const ROUTINE_MODIFYING_SQL_DATA_NOT_PERMITTED: &str = "2F002"; // modifying_sql_data_not_permitted
pub const ROUTINE_PROHIBITED_SQL_STATEMENT_ATTEMPTED: &str = "2F003"; // prohibited_sql_statement_attempted
pub const ROUTINE_READING_SQL_DATA_NOT_PERMITTED: &str = "2F004"; // reading_sql_data_not_permitted
// Class 34 — Invalid Cursor Name
pub const INVALID_CURSOR_NAME: &str = "34000"; // invalid_cursor_name
// Class 38 — External Routine Exception
pub const EXTERNAL_ROUTINE_EXCEPTION: &str = "38000"; // external_routine_exception
pub const EXTERNAL_ROUTINE_CONTAINING_SQL_NOT_PERMITTED: &str = "38001"; // containing_sql_not_permitted
pub const EXTERNAL_ROUTINE_MODIFYING_SQL_DATA_NOT_PERMITTED: &str = "38002"; // modifying_sql_data_not_permitted
pub const EXTERNAL_ROUTINE_PROHIBITED_SQL_STATEMENT_ATTEMPTED: &str = "38003"; // prohibited_sql_statement_attempted
pub const EXTERNAL_ROUTINE_READING_SQL_DATA_NOT_PERMITTED: &str = "38004"; // reading_sql_data_not_permitted
// Class 39 — External Routine Invocation Exception
pub const EXTERNAL_ROUTINE_INVOCATION_EXCEPTION: &str = "39000"; // external_routine_invocation_exception
pub const EXTERNAL_ROUTINE_INVALID_SQL_STATE_RETURNED: &str = "39001"; // invalid_sqlstate_returned
pub const EXTERNAL_ROUTINE_NULL_VALUE_NOT_ALLOWED: &str = "39004"; // null_value_not_allowed
pub const EXTERNAL_ROUTINE_TRIGGER_PROTOCOL_VIOLATED: &str = "39P01"; // trigger_protocol_violated
pub const EXTERNAL_ROUTINE_S_R_F_PROTOCOL_VIOLATED: &str = "39P02"; // srf_protocol_violated
pub const EXTERNAL_ROUTINE_EVENT_TRIGGER_PROTOCOL: &str = "39P03"; // event_trigger_protocol_violated
// Class 3B — Savepoint Exception
pub const SAVEPOINT_EXCEPTION: &str = "3B000"; // savepoint_exception
pub const INVALID_SAVEPOINT_SPECIFICATION: &str = "3B001"; // invalid_savepoint_specification
// Class 3D — Invalid Catalog Name
pub const INVALID_CATALOG_NAME: &str = "3D000"; // invalid_catalog_name
// Class 3F — Invalid Schema Name
pub const INVALID_SCHEMA_NAME: &str = "3F000"; // invalid_schema_name
// Class 40 — Transaction Rollback
pub const TRANSACTION_ROLLBACK: &str = "40000"; // transaction_rollback
pub const TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION: &str = "40002"; // transaction_integrity_constraint_violation
pub const SERIALIZATION_FAILURE: &str = "40001"; // serialization_failure
pub const STATEMENT_COMPLETION_UNKNOWN: &str = "40003"; // statement_completion_unknown
pub const DEADLOCK_DETECTED: &str = "40P01"; // deadlock_detected
// Class 42 — Syntax Error or Access Rule Violation
pub const SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: &str = "42000"; // syntax_error_or_access_rule_violation
pub const SYNTAX_ERROR: &str = "42601"; // syntax_error
pub const INSUFFICIENT_PRIVILEGE: &str = "42501"; // insufficient_privilege
pub const CANNOT_COERCE: &str = "42846"; // cannot_coerce
pub const GROUPING_ERROR: &str = "42803"; // grouping_error
pub const WINDOWING_ERROR: &str = "42P20"; // windowing_error
pub const INVALID_RECURSION: &str = "42P19"; // invalid_recursion
pub const INVALID_FOREIGN_KEY: &str = "42830"; // invalid_foreign_key
pub const INVALID_NAME: &str = "42602"; // invalid_name
pub const NAME_TOO_LONG: &str = "42622"; // name_too_long
pub const RESERVED_NAME: &str = "42939"; // reserved_name
pub const DATATYPE_MISMATCH: &str = "42804"; // datatype_mismatch
pub const INDETERMINATE_DATATYPE: &str = "42P18"; // indeterminate_datatype
pub const COLLATION_MISMATCH: &str = "42P21"; // collation_mismatch
pub const INDETERMINATE_COLLATION: &str = "42P22"; // indeterminate_collation
pub const WRONG_OBJECT_TYPE: &str = "42809"; // wrong_object_type
pub const UNDEFINED_COLUMN: &str = "42703"; // undefined_column
pub const UNDEFINED_FUNCTION: &str = "42883"; // undefined_function
pub const UNDEFINED_TABLE: &str = "42P01"; // undefined_table
pub const UNDEFINED_PARAMETER: &str = "42P02"; // undefined_parameter
pub const UNDEFINED_OBJECT: &str = "42704"; // undefined_object
pub const DUPLICATE_COLUMN: &str = "42701"; // duplicate_column
pub const DUPLICATE_CURSOR: &str = "42P03"; // duplicate_cursor
pub const DUPLICATE_DATABASE: &str = "42P04"; // duplicate_database
pub const DUPLICATE_FUNCTION: &str = "42723"; // duplicate_function
pub const DUPLICATE_PREPARED_STATEMENT: &str = "42P05"; // duplicate_prepared_statement
pub const DUPLICATE_SCHEMA: &str = "42P06"; // duplicate_schema
pub const DUPLICATE_TABLE: &str = "42P07"; // duplicate_table
pub const DUPLICATE_ALIAS: &str = "42712"; // duplicate_alias
pub const DUPLICATE_OBJECT: &str = "42710"; // duplicate_object
pub const AMBIGUOUS_COLUMN: &str = "42702"; // ambiguous_column
pub const AMBIGUOUS_FUNCTION: &str = "42725"; // ambiguous_function
pub const AMBIGUOUS_PARAMETER: &str = "42P08"; // ambiguous_parameter
pub const AMBIGUOUS_ALIAS: &str = "42P09"; // ambiguous_alias
pub const INVALID_COLUMN_REFERENCE: &str = "42P10"; // invalid_column_reference
pub const INVALID_COLUMN_DEFINITION: &str = "42611"; // invalid_column_definition
pub const INVALID_CURSOR_DEFINITION: &str = "42P11"; // invalid_cursor_definition
pub const INVALID_DATABASE_DEFINITION: &str = "42P12"; // invalid_database_definition
pub const INVALID_FUNCTION_DEFINITION: &str = "42P13"; // invalid_function_definition
pub const INVALID_STATEMENT_DEFINITION: &str = "42P14"; // invalid_prepared_statement_definition
pub const INVALID_SCHEMA_DEFINITION: &str = "42P15"; // invalid_schema_definition
pub const INVALID_TABLE_DEFINITION: &str = "42P16"; // invalid_table_definition
pub const INVALID_OBJECT_DEFINITION: &str = "42P17"; // invalid_object_definition
// Class 44 — WITH CHECK OPTION Violation
pub const WITH_CHECK_OPTION_VIOLATION: &str = "44000"; // with_check_option_violation
// Class 53 — Insufficient Resources
pub const INSUFFICIENT_RESOURCES: &str = "53000"; // insufficient_resources
pub const DISK_FULL: &str = "53100"; // disk_full
pub const OUT_OF_MEMORY: &str = "53200"; // out_of_memory
pub const TOO_MANY_CONNECTIONS: &str = "53300"; // too_many_connections
pub const CONFIGURATION_LIMIT_EXCEEDED: &str = "53400"; // configuration_limit_exceeded
// Class 54 — Program Limit Exceeded
pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000"; // program_limit_exceeded
pub const STATEMENT_TOO_COMPLEX: &str = "54001"; // statement_too_complex
pub const TOO_MANY_COLUMNS: &str = "54011"; // too_many_columns
pub const TOO_MANY_ARGUMENTS: &str = "54023"; // too_many_arguments
// Class 55 — Object Not In Prerequisite State
pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000"; // object_not_in_prerequisite_state
pub const OBJECT_IN_USE: &str = "55006"; // object_in_use
pub const CANT_CHANGE_RUNTIME_PARAM: &str = "55P02"; // cant_change_runtime_param
pub const LOCK_NOT_AVAILABLE: &str = "55P03"; // lock_not_available
// Class 57 — Operator Intervention
pub const OPERATOR_INTERVENTION: &str = "57000"; // operator_intervention
pub const QUERY_CANCELED: &str = "57014"; // query_canceled
pub const ADMIN_SHUTDOWN: &str = "57P01"; // admin_shutdown
pub const CRASH_SHUTDOWN: &str = "57P02"; // crash_shutdown
pub const CANNOT_CONNECT_NOW: &str = "57P03"; // cannot_connect_now
pub const DATABASE_DROPPED: &str = "57P04"; // database_dropped
// Class 58 — System Error (errors external to PostgreSQL itself)
pub const SYSTEM_ERROR: &str = "58000"; // system_error
pub const IO_ERROR: &str = "58030"; // io_error
pub const UNDEFINED_FILE: &str = "58P01"; // undefined_file
pub const DUPLICATE_FILE: &str = "58P02"; // duplicate_file
// Class 72 — Snapshot Failure
pub const SNAPSHOT_TOO_OLD: &str = "72000"; // snapshot_too_old
// Class F0 — Configuration file Error
pub const CONFIG_FILE_ERROR: &str = "F0000"; // config_file_error
pub const LOCK_FILE_EXISTS: &str = "F0001"; // lock_file_exists
// Class HV — Foreign Data Wrapper Error (SQL/MED)
pub const FDW_ERROR: &str = "HV000"; // fdw_error
pub const FDW_COLUMN_NAME_NOT_FOUND: &str = "HV005"; // fdw_column_name_not_found
pub const FDW_DYNAMIC_PARAMETER_VALUE_NEEDED: &str = "HV002"; // fdw_dynamic_parameter_value_needed
pub const FDW_FUNCTION_SEQUENCE_ERROR: &str = "HV010"; // fdw_function_sequence_error
pub const FDW_INCONSISTENT_DESCRIPTOR_INFORMATION: &str = "HV021"; // fdw_inconsistent_descriptor_information
pub const FDW_INVALID_ATTRIBUTE_VALUE: &str = "HV024"; // fdw_invalid_attribute_value
pub const FDW_INVALID_COLUMN_NAME: &str = "HV007"; // fdw_invalid_column_name
pub const FDW_INVALID_COLUMN_NUMBER: &str = "HV008"; // fdw_invalid_column_number
pub const FDW_INVALID_DATA_TYPE: &str = "HV004"; // fdw_invalid_data_type
pub const FDW_INVALID_DATA_TYPE_DESCRIPTORS: &str = "HV006"; // fdw_invalid_data_type_descriptors
pub const FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER: &str = "HV091"; // fdw_invalid_descriptor_field_identifier
pub const FDW_INVALID_HANDLE: &str = "HV00B"; // fdw_invalid_handle
pub const FDW_INVALID_OPTION_INDEX: &str = "HV00C"; // fdw_invalid_option_index
pub const FDW_INVALID_OPTION_NAME: &str = "HV00D"; // fdw_invalid_option_name
pub const FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH: &str = "HV090"; // fdw_invalid_string_length_or_buffer_length
pub const FDW_INVALID_STRING_FORMAT: &str = "HV00A"; // fdw_invalid_string_format
pub const FDW_INVALID_USE_OF_NULL_POINTER: &str = "HV009"; // fdw_invalid_use_of_null_pointer
pub const FDW_TOO_MANY_HANDLES: &str = "HV014"; // fdw_too_many_handles
pub const FDW_OUT_OF_MEMORY: &str = "HV001"; // fdw_out_of_memory
pub const FDW_NO_SCHEMAS: &str = "HV00P"; // fdw_no_schemas
pub const FDW_OPTION_NAME_NOT_FOUND: &str = "HV00J"; // fdw_option_name_not_found
pub const FDW_REPLY_HANDLE: &str = "HV00K"; // fdw_reply_handle
pub const FDW_SCHEMA_NOT_FOUND: &str = "HV00Q"; // fdw_schema_not_found
pub const FDW_TABLE_NOT_FOUND: &str = "HV00R"; // fdw_table_not_found
pub const FDW_UNABLE_TO_CREATE_EXECUTION: &str = "HV00L"; // fdw_unable_to_create_execution
pub const FDW_UNABLE_TO_CREATE_REPLY: &str = "HV00M"; // fdw_unable_to_create_reply
pub const FDW_UNABLE_TO_ESTABLISH_CONNECTION: &str = "HV00N"; // fdw_unable_to_establish_connection
// Class P0 — PL/pgSQL Error
pub const PLPGSQL_ERROR: &str = "P0000"; // plpgsql_error
pub const RAISE_EXCEPTION: &str = "P0001"; // raise_exception
pub const NO_DATA_FOUND: &str = "P0002"; // no_data_found
pub const TOO_MANY_ROWS: &str = "P0003"; // too_many_rows
pub const ASSERT_FAILURE: &str = "P0004"; // assert_failure
// Class XX — Internal Error
pub const INTERNAL_ERROR: &str = "XX000"; // internal_error
pub const DATA_CORRUPTED: &str = "XX001"; // data_corrupted
pub const INDEX_CORRUPTED: &str = "XX002"; // index_corrupted