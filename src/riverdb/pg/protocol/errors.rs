use strum::{Display, EnumString};

use crate::riverdb::{Error, Result};

/// ErrorCode is an enum of known Postgres error codes
#[derive(Display, EnumString)]
#[non_exhaustive]
pub enum ErrorCode {
    // Class 00 — Successful Completion
    #[strum(serialize = "00000")] // successful_completion
    SuccessfulCompletion,
    // Class 01 — Warning
    #[strum(serialize = "01000")] // warning
    Warning,
    #[strum(serialize = "0100C")] // dynamic_result_sets_returned
    WarningDynamicResultSetsReturned,
    #[strum(serialize = "01008")] // implicit_zero_bit_padding
    WarningImplicitZeroBitPadding,
    #[strum(serialize = "01003")] // null_value_eliminated_in_set_function
    WarningNullValueEliminatedInSetFunction,
    #[strum(serialize = "01007")] // privilege_not_granted
    WarningPrivilegeNotGranted,
    #[strum(serialize = "01006")] // privilege_not_revoked
    WarningPrivilegeNotRevoked,
    #[strum(serialize = "01004")] // string_data_right_truncation
    WarningStringDataRightTruncation,
    #[strum(serialize = "01P01")] // deprecated_feature
    WarningDeprecatedFeature,
    // Class 02 — No Data (this is also a warning class per the SQL standard)
    #[strum(serialize = "02000")] // no_data
    NoData,
    #[strum(serialize = "02001")] // no_additional_dynamic_result_sets_returned
    NoAdditionalDynamicResultSetsReturned,
    // Class 03 — SQL Statement Not Yet Complete
    #[strum(serialize = "03000")] // sql_statement_not_yet_complete
    SQLStatementNotYetComplete,
    // Class 08 — Connection Exception
    #[strum(serialize = "08000")] // connection_exception
    ConnectionException,
    #[strum(serialize = "08003")] // connection_does_not_exist
    ConnectionDoesNotExist,
    #[strum(serialize = "08006")] // connection_failure
    ConnectionFailure,
    #[strum(serialize = "08001")] // sqlclient_unable_to_establish_sqlconnection
    SQLClientUnableToEstablishSQLConnection,
    #[strum(serialize = "08004")] // sqlserver_rejected_establishment_of_sqlconnection
    SQLServerRejectedEstablishementOfSQLConnection,
    #[strum(serialize = "08007")] // transaction_resolution_unknown
    TransactionResolutionUnknown,
    #[strum(serialize = "08P01")] // protocol_violation
    ProtocolViolation,
    // Class 09 — Triggered Action Exception
    #[strum(serialize = "09000")] // triggered_action_exception
    TriggeredActionException,
    // Class 0A — Feature Not Supported
    #[strum(serialize = "0A000")] // feature_not_supported
    FeatureNotSupported,
    // Class 0B — Invalid Transaction Initiation
    #[strum(serialize = "0B000")] // invalid_transaction_initiation
    InvalidTransactionInitiation,
    // Class 0F — Locator Exception
    #[strum(serialize = "0F000")] // locator_exception
    LocatorException,
    #[strum(serialize = "0F001")] // invalid_locator_specification
    InvalidLocatorSpecification,
    // Class 0L — Invalid Grantor
    #[strum(serialize = "0L000")] // invalid_grantor
    InvalidGrantor,
    #[strum(serialize = "0LP01")] // invalid_grant_operation
    InvalidGrantOperation,
    // Class 0P — Invalid Role Specification
    #[strum(serialize = "0P000")] // invalid_role_specification
    InvalidRoleSpecification,
    // Class 0Z — Diagnostics Exception
    #[strum(serialize = "0Z000")] // diagnostics_exception
    DiagnosticsException,
    #[strum(serialize = "0Z002")] // stacked_diagnostics_accessed_without_active_handler
    StackedDiagnosticsAccessedWithoutActiveHandler,
    // Class 20 — Case Not Found
    #[strum(serialize = "20000")] // case_not_found
    CaseNotFound,
    // Class 21 — Cardinality Violation
    #[strum(serialize = "21000")] // cardinality_violation
    CardinalityViolation,
    // Class 22 — Data Exception
    #[strum(serialize = "22000")] // data_exception
    DataException,
    #[strum(serialize = "2202E")] // array_subscript_error
    ArraySubscriptError,
    #[strum(serialize = "22021")] // character_not_in_repertoire
    CharacterNotInRepertoire,
    #[strum(serialize = "22008")] // datetime_field_overflow
    DatatimeFieldOverflow,
    #[strum(serialize = "22012")] // division_by_zero
    DivisionByZero,
    #[strum(serialize = "22005")] // error_in_assignment
    ErrorInAssignment,
    #[strum(serialize = "2200B")] // escape_character_conflict
    EscapeCharacterConflict,
    #[strum(serialize = "22022")] // indicator_overflow
    IndicatorOverflow,
    #[strum(serialize = "22015")] // interval_field_overflow
    IntervalFieldOverflow,
    #[strum(serialize = "2201E")] // invalid_argument_for_logarithm
    InvalidArgumentForLogarithm,
    #[strum(serialize = "22014")] // invalid_argument_for_ntile_function
    InvalidArgumentForNTileFunction,
    #[strum(serialize = "22016")] // invalid_argument_for_nth_value_function
    InvalidArgumentForNthValueFunction,
    #[strum(serialize = "2201F")] // invalid_argument_for_power_function
    InvalidArgumentForPowerFunction,
    #[strum(serialize = "2201G")] // invalid_argument_for_width_bucket_function
    InvalidArgumentForWidthBucketFunction,
    #[strum(serialize = "22018")] // invalid_character_value_for_cast
    InvalidCharacterValueForCast,
    #[strum(serialize = "22007")] // invalid_datetime_format
    InvalidDatatimeFormat,
    #[strum(serialize = "22019")] // invalid_escape_character
    InvalidEscapeCharacter,
    #[strum(serialize = "2200D")] // invalid_escape_octet
    InvalidEscapeOctet,
    #[strum(serialize = "22025")] // invalid_escape_sequence
    InvalidEscapeSequence,
    #[strum(serialize = "22P06")] // nonstandard_use_of_escape_character
    NonStandardUseOfEscapeCharacter,
    #[strum(serialize = "22010")] // invalid_indicator_parameter_value
    ErrorcodeInvalidIndicatorParameterValue,
    #[strum(serialize = "22023")] // invalid_parameter_value
    InvalidParameterValue,
    #[strum(serialize = "2201B")] // invalid_regular_expression
    InvalidRegularExpression,
    #[strum(serialize = "2201W")] // invalid_row_count_in_limit_clause
    InvalidRowCountInLimitClause,
    #[strum(serialize = "2201X")] // invalid_row_count_in_result_offset_clause
    InvalidRowCountInResultOffsetClause,
    #[strum(serialize = "2202H")] // invalid_tablesample_argument
    InvalidTablesampleArgument,
    #[strum(serialize = "2202G")] // invalid_tablesample_repeat
    InvalidTablesampleRepeat,
    #[strum(serialize = "22009")] // invalid_time_zone_displacement_value
    InvalidTimeZoneDisplacementValue,
    #[strum(serialize = "2200C")] // invalid_use_of_escape_character
    InvalidInvalidUseOfEscapeCharacter,
    #[strum(serialize = "2200G")] // most_specific_type_mismatch
    MostSpecificTypeMismatch,
    #[strum(serialize = "22004")] // null_value_not_allowed
    NullValueNotAllowed,
    #[strum(serialize = "22002")] // null_value_no_indicator_parameter
    NullValueNoIndicatorParameter,
    #[strum(serialize = "22003")] // numeric_value_out_of_range
    NumericValueOutOfRange,
    #[strum(serialize = "22026")] // string_data_length_mismatch
    StringDataLengthMismatch,
    #[strum(serialize = "22001")] // string_data_right_truncation
    StringDataRightTruncation,
    #[strum(serialize = "22011")] // substring_error
    SubstringError,
    #[strum(serialize = "22027")] // trim_error
    TrimError,
    #[strum(serialize = "22024")] // unterminated_c_string
    UntermincatedCString,
    #[strum(serialize = "2200F")] // zero_length_character_string
    ZeroLengthCharacterString,
    #[strum(serialize = "22P01")] // floating_point_exception
    FloatingPointException,
    #[strum(serialize = "22P02")] // invalid_text_representation
    InvalidTextRepresentation,
    #[strum(serialize = "22P03")] // invalid_binary_representation
    InvalidBinaryRepresentation,
    #[strum(serialize = "22P04")] // bad_copy_file_format
    BadCopyFileFormat,
    #[strum(serialize = "22P05")] // untranslatable_character
    UnstranslatableCharacter,
    #[strum(serialize = "2200L")] // not_an_xml_document
    NotAnXMLDocument,
    #[strum(serialize = "2200M")] // invalid_xml_document
    InvalideXMLDocument,
    #[strum(serialize = "2200N")] // invalid_xml_content
    InvalidXMLContent,
    #[strum(serialize = "2200S")] // invalid_xml_comment
    InvalidXMLComment,
    #[strum(serialize = "2200T")] // invalid_xml_processing_instruction
    InvalidXMLProcessingInstruction,
    // // Class 23 — Integrity Constraint Violation
    #[strum(serialize = "23000")] // integrity_constraint_violation
    IntegrityConstraintViolation,
    #[strum(serialize = "23001")] // restrict_violation
    RestrictViolation,
    #[strum(serialize = "23502")] // not_null_violation
    NotNullViolation,
    #[strum(serialize = "23503")] // foreign_key_violation
    ForeignKeyViolation,
    #[strum(serialize = "23505")] // unique_violation
    UniqueViolation,
    #[strum(serialize = "23514")] // check_violation
    CheckViolation,
    #[strum(serialize = "23P01")] // exclusion_violation
    ExclusionViolation,
    // // Class 24 — Invalid Cursor State
    #[strum(serialize = "24000")] // invalid_cursor_state
    InvalidCursorState,
    // // Class 25 — Invalid Transaction State
    #[strum(serialize = "25000")] // invalid_transaction_state
    InvalidTransactionState,
    #[strum(serialize = "25001")] // active_sql_transaction
    ActiveSQLTransaction,
    #[strum(serialize = "25002")] // branch_transaction_already_active
    BranchTransactionAlreadyActive,
    #[strum(serialize = "25008")] // held_cursor_requires_same_isolation_level
    HeldCursorRequiresSameIsolationLevel,
    #[strum(serialize = "25003")] // inappropriate_access_mode_for_branch_transaction
    InappropriateAccessModeForBranchTransaction,
    #[strum(serialize = "25004")] // inappropriate_isolation_level_for_branch_transaction
    InappropriateIsolationLevelForBranchTransaction,
    #[strum(serialize = "25005")] // no_active_sql_transaction_for_branch_transaction
    NoActiveSQLTransactionForBranchTransaction,
    #[strum(serialize = "25006")] // read_only_sql_transaction
    ReadOnlySQLTransaction,
    #[strum(serialize = "25007")] // schema_and_data_statement_mixing_not_supported
    SchemaAndDataStatementMixingNotSupported,
    #[strum(serialize = "25P01")] // no_active_sql_transaction
    NoActiveSQLTransaction,
    #[strum(serialize = "25P02")] // in_failed_sql_transaction
    InFailedSQLTransaction,
    #[strum(serialize = "25P03")] // idle_in_transaction_session_timeout
    IdleInTransactionSessionTimeout,
    // Class 26 — Invalid SQL Statement Name
    #[strum(serialize = "26000")] // invalid_sql_statement_name
    InvalidSQLStatementName,
    // Class 27 — Triggered Data Change Violation
    #[strum(serialize = "27000")] // triggered_data_change_violation
    TriggeredDataChangeViolation,
    // Class 28 — Invalid Authorization Specification
    #[strum(serialize = "28000")] // invalid_authorization_specification
    InvalidAuthorizationSpecification,
    #[strum(serialize = "28P01")] // invalid_password
    InvalidPassword,
    // Class 2B — Dependent Privilege Descriptors Still Exist
    #[strum(serialize = "2B000")] // dependent_privilege_descriptors_still_exist
    DependentPrivilegeDescriptorsStillExist,
    #[strum(serialize = "2BP01")] // dependent_objects_still_exist
    DependentObjectsStillExist,
    // Class 2D — Invalid Transaction Termination
    #[strum(serialize = "2D000")] // invalid_transaction_termination
    InvalidTransactionTermination,
    // Class 2F — SQL Routine Exception
    #[strum(serialize = "2F000")] // sql_routine_exception
    RoutineSQLRuntimeException,
    #[strum(serialize = "2F005")] // function_executed_no_return_statement
    RoutineFunctionExecutedNoReturnStatement,
    #[strum(serialize = "2F002")] // modifying_sql_data_not_permitted
    RoutineModifyingSQLDataNotPermitted,
    #[strum(serialize = "2F003")] // prohibited_sql_statement_attempted
    RoutineProhibitedSQLStatementAttempted,
    #[strum(serialize = "2F004")] // reading_sql_data_not_permitted
    RoutineReadingSQLDataNotPermitted,
    // Class 34 — Invalid Cursor Name
    #[strum(serialize = "34000")] // invalid_cursor_name
    InvalidCursorName,
    // Class 38 — External Routine Exception
    #[strum(serialize = "38000")] // external_routine_exception
    ExternalRoutineException,
    #[strum(serialize = "38001")] // containing_sql_not_permitted
    ExternalRoutineContainingSQLNotPermitted,
    #[strum(serialize = "38002")] // modifying_sql_data_not_permitted
    ExternalRoutineModifyingSQLDataNotPermitted,
    #[strum(serialize = "38003")] // prohibited_sql_statement_attempted
    ExternalRoutineProhibitedSQLStatementAttempted,
    #[strum(serialize = "38004")] // reading_sql_data_not_permitted
    ExternalRoutineReadingSQLDataNotPermitted,
    // Class 39 — External Routine Invocation Exception
    #[strum(serialize = "39000")] // external_routine_invocation_exception
    ExternalRoutineInvocationException,
    #[strum(serialize = "39001")] // invalid_sqlstate_returned
    ExternalRoutineInvalidSQLStateReturned,
    #[strum(serialize = "39004")] // null_value_not_allowed
    ExternalRoutineNullValueNotAllowed,
    #[strum(serialize = "39P01")] // trigger_protocol_violated
    ExternalRoutineTriggerProtocolViolated,
    #[strum(serialize = "39P02")] // srf_protocol_violated
    ExternalRoutineSRFProtocolViolated,
    #[strum(serialize = "39P03")] // event_trigger_protocol_violated
    ExternalRoutineEventTriggerProtocol,
    // Class 3B — Savepoint Exception
    #[strum(serialize = "3B000")] // savepoint_exception
    SavepointException,
    #[strum(serialize = "3B001")] // invalid_savepoint_specification
    InvalidSavepointSpecification,
    // Class 3D — Invalid Catalog Name
    #[strum(serialize = "3D000")] // invalid_catalog_name
    InvalidCatalogName,
    // Class 3F — Invalid Schema Name
    #[strum(serialize = "3F000")] // invalid_schema_name
    InvalidSchemaName,
    // Class 40 — Transaction Rollback
    #[strum(serialize = "40000")] // transaction_rollback
    TransactionRollback,
    #[strum(serialize = "40002")] // transaction_integrity_constraint_violation
    TransactionIntegrityConstraintViolation,
    #[strum(serialize = "40001")] // serialization_failure
    SerializationFailure,
    #[strum(serialize = "40003")] // statement_completion_unknown
    StatementCompletionUnknown,
    #[strum(serialize = "40P01")] // deadlock_detected
    DeadlockDetected,
    // Class 42 — Syntax Error or Access Rule Violation
    #[strum(serialize = "42000")] // syntax_error_or_access_rule_violation
    SyntaxErrorOrAccessRuleViolation,
    #[strum(serialize = "42601")] // syntax_error
    SyntaxError,
    #[strum(serialize = "42501")] // insufficient_privilege
    InsufficientPrivilege,
    #[strum(serialize = "42846")] // cannot_coerce
    CannotCoerce,
    #[strum(serialize = "42803")] // grouping_error
    GroupingError,
    #[strum(serialize = "42P20")] // windowing_error
    WindowingError,
    #[strum(serialize = "42P19")] // invalid_recursion
    InvalidRecursion,
    #[strum(serialize = "42830")] // invalid_foreign_key
    InvalidForeignKey,
    #[strum(serialize = "42602")] // invalid_name
    InvalidName,
    #[strum(serialize = "42622")] // name_too_long
    NameTooLong,
    #[strum(serialize = "42939")] // reserved_name
    ReservedName,
    #[strum(serialize = "42804")] // datatype_mismatch
    DatatypeMismatch,
    #[strum(serialize = "42P18")] // indeterminate_datatype
    IndeterminateDatatype,
    #[strum(serialize = "42P21")] // collation_mismatch
    CollationMismatch,
    #[strum(serialize = "42P22")] // indeterminate_collation
    IndeterminateCollation,
    #[strum(serialize = "42809")] // wrong_object_type
    WrongObjectType,
    #[strum(serialize = "42703")] // undefined_column
    UndefinedColumn,
    #[strum(serialize = "42883")] // undefined_function
    UndefinedFunction,
    #[strum(serialize = "42P01")] // undefined_table
    UndefinedTable,
    #[strum(serialize = "42P02")] // undefined_parameter
    UndefinedParameter,
    #[strum(serialize = "42704")] // undefined_object
    UndefinedObject,
    #[strum(serialize = "42701")] // duplicate_column
    DuplicateColumn,
    #[strum(serialize = "42P03")] // duplicate_cursor
    DuplicateCursor,
    #[strum(serialize = "42P04")] // duplicate_database
    DuplicateDatabase,
    #[strum(serialize = "42723")] // duplicate_function
    DuplicateFunction,
    #[strum(serialize = "42P05")] // duplicate_prepared_statement
    DuplicatePreparedStatement,
    #[strum(serialize = "42P06")] // duplicate_schema
    DuplicateSchema,
    #[strum(serialize = "42P07")] // duplicate_table
    DuplicateTable,
    #[strum(serialize = "42712")] // duplicate_alias
    DuplicateAlias,
    #[strum(serialize = "42710")] // duplicate_object
    DuplicateObject,
    #[strum(serialize = "42702")] // ambiguous_column
    AmbiguousColumn,
    #[strum(serialize = "42725")] // ambiguous_function
    AmbiguousFunction,
    #[strum(serialize = "42P08")] // ambiguous_parameter
    AmbiguousParameter,
    #[strum(serialize = "42P09")] // ambiguous_alias
    AmbiguousAlias,
    #[strum(serialize = "42P10")] // invalid_column_reference
    InvalidColumnReference,
    #[strum(serialize = "42611")] // invalid_column_definition
    InvalidColumnDefinition,
    #[strum(serialize = "42P11")] // invalid_cursor_definition
    InvalidCursorDefinition,
    #[strum(serialize = "42P12")] // invalid_database_definition
    InvalidDatabaseDefinition,
    #[strum(serialize = "42P13")] // invalid_function_definition
    InvalidFunctionDefinition,
    #[strum(serialize = "42P14")] // invalid_prepared_statement_definition
    InvalidStatementDefinition,
    #[strum(serialize = "42P15")] // invalid_schema_definition
    InvalidSchemaDefinition,
    #[strum(serialize = "42P16")] // invalid_table_definition
    InvalidTableDefinition,
    #[strum(serialize = "42P17")] // invalid_object_definition
    InvalidObjectDefinition,
    // Class 44 — WITH CHECK OPTION Violation
    #[strum(serialize = "44000")] // with_check_option_violation
    WithCheckOptionViolation,
    // Class 53 — Insufficient Resources
    #[strum(serialize = "53000")] // insufficient_resources
    InsufficientResources,
    #[strum(serialize = "53100")] // disk_full
    DiskFull,
    #[strum(serialize = "53200")] // out_of_memory
    OutOfMemory,
    #[strum(serialize = "53300")] // too_many_connections
    TooManyConnections,
    #[strum(serialize = "53400")] // configuration_limit_exceeded
    ConfigurationLimitExceeded,
    // Class 54 — Program Limit Exceeded
    #[strum(serialize = "54000")] // program_limit_exceeded
    ProgramLimitExceeded,
    #[strum(serialize = "54001")] // statement_too_complex
    StatementTooComplex,
    #[strum(serialize = "54011")] // too_many_columns
    TooManyColumns,
    #[strum(serialize = "54023")] // too_many_arguments
    TooManyArguments,
    // Class 55 — Object Not In Prerequisite State
    #[strum(serialize = "55000")] // object_not_in_prerequisite_state
    ObjectNotInPrerequisiteState,
    #[strum(serialize = "55006")] // object_in_use
    ObjectInUse,
    #[strum(serialize = "55P02")] // cant_change_runtime_param
    CantChangeRuntimeParam,
    #[strum(serialize = "55P03")] // lock_not_available
    LockNotAvailable,
    // Class 57 — Operator Intervention
    #[strum(serialize = "57000")] // operator_intervention
    OperatorIntervention,
    #[strum(serialize = "57014")] // query_canceled
    QueryCanceled,
    #[strum(serialize = "57P01")] // admin_shutdown
    AdminShutdown,
    #[strum(serialize = "57P02")] // crash_shutdown
    CrashShutdown,
    #[strum(serialize = "57P03")] // cannot_connect_now
    CannotConnectNow,
    #[strum(serialize = "57P04")] // database_dropped
    DatabaseDropped,
    // Class 58 — System Error (errors external to PostgreSQL itself)
    #[strum(serialize = "58000")] // system_error
    SystemError,
    #[strum(serialize = "58030")] // io_error
    IOError,
    #[strum(serialize = "58P01")] // undefined_file
    UndefinedFile,
    #[strum(serialize = "58P02")] // duplicate_file
    DuplicateFile,
    // Class 72 — Snapshot Failure
    #[strum(serialize = "72000")] // snapshot_too_old
    SnapshotTooOld,
    // Class F0 — Configuration file Error
    #[strum(serialize = "F0000")] // config_file_error
    ConfigFileError,
    #[strum(serialize = "F0001")] // lock_file_exists
    LockFileExists,
    // Class HV — Foreign Data Wrapper Error (SQL/MED)
    #[strum(serialize = "HV000")] // fdw_error
    FDWError,
    #[strum(serialize = "HV005")] // fdw_column_name_not_found
    FDWColumnNameNotFound,
    #[strum(serialize = "HV002")] // fdw_dynamic_parameter_value_needed
    FDWDynamicParameterValueNeeded,
    #[strum(serialize = "HV010")] // fdw_function_sequence_error
    FDWFunctionSequenceError,
    #[strum(serialize = "HV021")] // fdw_inconsistent_descriptor_information
    FDWInconsistentDescriptorInformation,
    #[strum(serialize = "HV024")] // fdw_invalid_attribute_value
    FDWInvalidAttributeValue,
    #[strum(serialize = "HV007")] // fdw_invalid_column_name
    FDWInvalidColumnName,
    #[strum(serialize = "HV008")] // fdw_invalid_column_number
    FDWInvalidColumnNumber,
    #[strum(serialize = "HV004")] // fdw_invalid_data_type
    FDWInvalidDataType,
    #[strum(serialize = "HV006")] // fdw_invalid_data_type_descriptors
    FDWInvalidDataTypeDescriptors,
    #[strum(serialize = "HV091")] // fdw_invalid_descriptor_field_identifier
    FDWInvalidDescriptorFieldIdentifier,
    #[strum(serialize = "HV00B")] // fdw_invalid_handle
    FDWInvalidHandle,
    #[strum(serialize = "HV00C")] // fdw_invalid_option_index
    FDWInvalidOptionIndex,
    #[strum(serialize = "HV00D")] // fdw_invalid_option_name
    FDWInvalidOptionName,
    #[strum(serialize = "HV090")] // fdw_invalid_string_length_or_buffer_length
    FDWInvalidStringLengthOrBufferLength,
    #[strum(serialize = "HV00A")] // fdw_invalid_string_format
    FDWInvalidStringFormat,
    #[strum(serialize = "HV009")] // fdw_invalid_use_of_null_pointer
    FDWInvalidUseOfNullPointer,
    #[strum(serialize = "HV014")] // fdw_too_many_handles
    FDWTooManyHandles,
    #[strum(serialize = "HV001")] // fdw_out_of_memory
    FDWOutOfMemory,
    #[strum(serialize = "HV00P")] // fdw_no_schemas
    FDWNoSchemas,
    #[strum(serialize = "HV00J")] // fdw_option_name_not_found
    FDWOptionNameNotFound,
    #[strum(serialize = "HV00K")] // fdw_reply_handle
    FDWReplyHandle,
    #[strum(serialize = "HV00Q")] // fdw_schema_not_found
    FDWSchemaNotFound,
    #[strum(serialize = "HV00R")] // fdw_table_not_found
    FDWTableNotFound,
    #[strum(serialize = "HV00L")] // fdw_unable_to_create_execution
    FDWUnableToCreateExecution,
    #[strum(serialize = "HV00M")] // fdw_unable_to_create_reply
    FDWUnableToCreateReply,
    #[strum(serialize = "HV00N")] // fdw_unable_to_establish_connection
    FDWUnableToEstablishConnection,
    // Class P0 — PL/pgSQL Error
    #[strum(serialize = "P0000")] // plpgsql_error
    PLPGSQLError,
    #[strum(serialize = "P0001")] // raise_exception
    RaiseException,
    #[strum(serialize = "P0002")] // no_data_found
    NoDataFound,
    #[strum(serialize = "P0003")] // too_many_rows
    TooManyRows,
    #[strum(serialize = "P0004")] // assert_failure
    AssertFailure,
    // Class XX — Internal Error
    #[strum(serialize = "XX000")] // internal_error
    InternalError,
    #[strum(serialize = "XX001")] // data_corrupted
    DataCorrupted,
    #[strum(serialize = "XX002")] // index_corrupted
    IndexCorrupted,
}

#[derive(Display, EnumString)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ErrorSeverity {
    Fatal,
    Panic,
    Error,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

#[derive(Display, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
#[non_exhaustive]
pub enum ErrorFieldTag {
    NullTerminator = 0,
    LocalizedSeverity = 'S' as u8,
    Severity = 'V' as u8,
    Code = 'C' as u8,
    Message = 'M' as u8,
    MessageDetail = 'D' as u8,
    MessageHint = 'H' as u8,
    Position = 'P' as u8,
    InternalPosition = 'p' as u8,
    InternalQuery = 'q' as u8,
    Where = 'W' as u8,
    SchemaName = 's' as u8,
    TableName = 't' as u8,
    ColumnName = 'c' as u8,
    DataTypeName = 'd' as u8,
    ConstraintName = 'n' as u8,
    File = 'F' as u8,
    Line = 'L' as u8,
    Routine = 'R' as u8,
}

impl ErrorFieldTag {
    pub fn new(b: u8) -> Result<Self> {
        let tag = unsafe { Self::new_unchecked(b) };
        tag.check().map(tag)
    }

    pub unsafe fn new_unchecked(b: u8) -> Self {
        std::mem::transmute(b)
    }

    pub fn check(&self) -> Result<()> {
        match self {
            ErrorFieldTag::NullTerminator |
            ErrorFieldTag::LocalizedSeverity |
            ErrorFieldTag::Severity |
            ErrorFieldTag::Code |
            ErrorFieldTag::Message |
            ErrorFieldTag::MessageDetail |
            ErrorFieldTag::MessageHint |
            ErrorFieldTag::Position |
            ErrorFieldTag::InternalPosition |
            ErrorFieldTag::InternalQuery |
            ErrorFieldTag::Where |
            ErrorFieldTag::SchemaName |
            ErrorFieldTag::TableName |
            ErrorFieldTag::ColumnName |
            ErrorFieldTag::DataTypeName |
            ErrorFieldTag::ConstraintName |
            ErrorFieldTag::File |
            ErrorFieldTag::Line |
            ErrorFieldTag::Routine => Ok(tag),
            _ => Err(Error::protocol_error(format!("unknown error field tag '{}'", b as char))),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}