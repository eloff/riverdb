use strum::Display;

#[derive(Display, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum QueryType {
    Other,
    Select,
    SelectInto,
    SelectWithLocking,
    Insert,
    Update,
    Delete,
    InsertReturning,
    UpdateReturning,
    DeleteReturning,
    With,
    Begin, // includes START
    Rollback, // includes ABORT
    RollbackPrepared,
    RollbackSavepoint,
    Commit, // includes END
    CommitPrepared,
    Show,
    SetConstraints,
    SetSession,
    SetRole, // and SET SESSION AUTHORIZATION
    SetLocal,
    SetTransaction,
    Reset,
    Alter,
    Create,
    Call,
    Copy,
    Drop,
    Do,
    Execute,
    Grant,
    Revoke,
    Prepare,
    PrepareTransaction,
    Cursor, // includes DECLARE, FETCH, MOVE, CLOSE
    Listen,
    Unlisten,
    Lock,
    Notify,
    Savepoint,
    ReleaseSavepoint, // not the same as RollbackSavepoint
    Truncate,
    Vacuum,
    Values,
}

impl From<&str> for QueryType {
    fn from(normalized_query: &str) -> Self {
        if normalized_query.is_empty() {
            return Self::Other;
        }
        match normalized_query.chars().next().unwrap() {
            'A' => {
                if normalized_query.starts_with("ALTER") {
                    return Self::Alter;
                } else if normalized_query.starts_with("ABORT") {
                    return Self::Rollback;
                }
            },
            'B' => {
                if normalized_query.starts_with("BEGIN") {
                    return Self::Begin;
                }
            },
            'C' => {
                if normalized_query.starts_with("COMMIT") {
                    return if (&normalized_query[6..]).trim_start().starts_with("PREPARED") {
                        Self::CommitPrepared
                    } else {
                        Self::Commit
                    };
                } else if normalized_query.starts_with("CALL") {
                    return Self::Call;
                } else if normalized_query.starts_with("CREATE") {
                    return Self::Create;
                } else if normalized_query.starts_with("COPY") {
                    return Self::Copy;
                } else if normalized_query.starts_with("CLOSE") {
                    return Self::Cursor;
                }
            },
            'D' => {
                if normalized_query.starts_with("DELETE") {
                    return if normalized_query.contains("RETURNING") {
                        Self::DeleteReturning
                    } else {
                        Self::Delete
                    };
                } else if normalized_query.starts_with("DROP") {
                    return Self::Drop;
                } else if normalized_query.starts_with("DECLARE") {
                    return Self::Cursor;
                } else if normalized_query.starts_with("DO") {
                    return Self::Do;
                }
            },
            'E' => {
                if normalized_query.starts_with("END") {
                    return Self::Commit;
                } else if normalized_query.starts_with("EXECUTE") {
                    return Self::Execute;
                }
            },
            'G' =>  {
                if normalized_query.starts_with("GRANT") {
                    return Self::Grant;
                }
            },
            'I' => {
                if normalized_query.starts_with("INSERT") {
                    return if normalized_query.contains("RETURNING") {
                        Self::InsertReturning
                    } else {
                        Self::Insert
                    };
                }
            },
            'L' => {
                if normalized_query.starts_with("LOCK") {
                    return Self::Lock;
                } else if normalized_query.starts_with("LISTEN") {
                    return Self::Listen;
                }
            },
            'M' => {
                if normalized_query.starts_with("MOVE") {
                    return Self::Cursor;
                }
            },
            'N' => {
                if normalized_query.starts_with("NOTIFY") {
                    return Self::Notify;
                }
            },
            'P' => {
                if normalized_query.starts_with("PREPARE") {
                    return if (&normalized_query[7..]).trim_start().starts_with("TRANSACTION") {
                        Self::PrepareTransaction
                    } else {
                        Self::Prepare
                    };
                }
            },
            'R' => {
                if normalized_query.starts_with("ROLLBACK") {
                    let next = (&normalized_query[8..]).trim_start();
                    return if next.starts_with("TO") {
                        Self::RollbackSavepoint
                    } else if next.starts_with("PREPARED") {
                        Self::RollbackPrepared
                    } else {
                        Self::Rollback
                    };
                } else if normalized_query.starts_with("REVOKE") {
                    return Self::Revoke;
                } else if normalized_query.starts_with("RESET") {
                    return Self::Reset;
                } else if normalized_query.starts_with("RELEASE") {
                    return Self::ReleaseSavepoint;
                }
            },
            'S' => {
                if normalized_query.starts_with("SELECT") {
                    return if (&normalized_query[6..]).trim_start().starts_with("INTO") {
                        Self::SelectInto
                    } else if normalized_query.contains(" FOR ") {
                        Self::SelectWithLocking
                    } else {
                        Self::Select
                    };
                } else if normalized_query.starts_with("SET") {
                    let next = (&normalized_query[3..]).trim_start();
                    return if next.starts_with("LOCAL") {
                        Self::SetLocal
                    } else if next.starts_with("CONSTRAINTS") {
                        Self::SetConstraints
                    } else if next.starts_with("TRANSACTION") {
                        Self::SetTransaction
                    } else if next.starts_with("ROLE") ||
                        (next.starts_with("SESSION") && (&next[7..]).starts_with("AUTHORIZATION")) {
                        Self::SetRole
                    } else {
                        Self::SetSession
                    };
                } else if normalized_query.starts_with("START") {
                    return Self::Begin;
                } else if normalized_query.starts_with("SHOW") {
                    return Self::Show;
                } else if normalized_query.starts_with("SAVEPOINT") {
                    return Self::Savepoint;
                }
            },
            'T' => {
                if normalized_query.starts_with("TRUNCATE") {
                    return Self::Truncate;
                }
            },
            'U' => {
                if normalized_query.starts_with("UPDATE") {
                    return if normalized_query.contains("RETURNING") {
                        Self::UpdateReturning
                    } else {
                        Self::Update
                    };
                } else if normalized_query.starts_with("UNLISTEN") {
                    return Self::Unlisten;
                }
            },
            'V' => {
                if normalized_query.starts_with("VACUUM") {
                    return Self::Vacuum;
                } else if normalized_query.starts_with("VALUES") {
                    return Self::Values;
                }
            },
            _ => (),
        }
        Self::Other
    }
}