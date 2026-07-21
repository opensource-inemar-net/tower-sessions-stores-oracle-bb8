use async_trait::async_trait;
use bb8::Pool;
use bb8::PooledConnection;
use bb8_oracle::OracleConnectionManager;
use time::OffsetDateTime;
use tower_sessions_core::ExpiredDeletion;
use tower_sessions_core::SessionStore;
use tower_sessions_core::session::Id;
use tower_sessions_core::session::Record;
use tower_sessions_core::session_store;
use tracing::debug;
use tracing::error;

/// A Oracle SQL session store.
#[derive(Clone, Debug)]
pub struct OracleStore {
    pool: Pool<OracleConnectionManager>,
    table_name: String,
}

impl OracleStore {
    /// Create a new OracleStore store with the provided connection pool.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_sqlx_store::{sqlx::any, OracleStore};
    ///
    /// # tokio_test::block_on(async {
    /// let database_url = std::option_env!("DATABASE_URL").unwrap();
    /// let pool = PgPool::connect(database_url).await.unwrap();
    /// let session_store = PostgresStore::new(pool);
    /// # })
    /// ```
    pub fn new(pool: Pool<OracleConnectionManager>) -> Self {
        Self {
            pool,
            table_name: "session".to_string(),
        }
    }

    /// Set the session table name with the provided name.
    pub fn with_table_name(mut self, table_name: impl AsRef<str>) -> Result<Self, String> {
        let table_name = table_name.as_ref();
        if !is_valid_identifier(table_name) {
            return Err(format!(
                "Invalid table name '{}'. Table names must start with a letter or underscore \
                 (including letters with diacritical marks and non-Latin letters).Subsequent \
                 characters can be letters, underscores, digits (0-9), or dollar signs ($).",
                table_name
            ));
        }

        table_name.clone_into(&mut self.table_name);
        Ok(self)
    }

    /// Migrate the session schema.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_sqlx_store::{sqlx::PgPool, PostgresStore};
    ///
    /// # tokio_test::block_on(async {
    /// let database_url = std::option_env!("DATABASE_URL").unwrap();
    /// let pool = PgPool::connect(database_url).await.unwrap();
    /// let session_store = PostgresStore::new(pool);
    /// session_store.migrate().await.unwrap();
    /// # })
    /// ```
    pub async fn migrate(&self) -> Result<(), OracleSessionStoreError> {
        let tx = self.pool.get().await?;
        debug!("Got Oracle connection");

        let create_table_query = format!(
            r#"
            create table if not exists {}  (
               myid varchar2(4000) primary key not null,
               data BLOB  not null,
               expiry_data NUMBER not null
            )"#,
            self.table_name
        );

        //let schema_name = self.schema_name.clone();
        //let table_name = "CPDTESTFLUX.sessions";
        let tx = tx.clone();
        let res: Result<(), OracleSessionStoreError> = tokio::task::spawn_blocking(move || {
            tx.execute(create_table_query.as_str(), &[])?;
            tx.commit()?;
            Ok(())
        })
        .await?;
        res
    }

    async fn id_exists(
        &self,
        conn: &mut PooledConnection<'_, OracleConnectionManager>,
        id: &Id,
    ) -> Result<bool, OracleSessionStoreError> {
        let query = format!(
            r#"
            select myid from {} where myid = :1
            "#,
            self.table_name
        );

        let id = id.to_string();
        let tx = conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut res = tx.query(query.as_str(), &[&id])?;
            match res.next() {
                Some(_row) => Ok(true),
                None => Ok(false),
            }
        })
        .await?
    }

    async fn save_with_conn(
        &self,
        conn: &mut PooledConnection<'_, OracleConnectionManager>,
        record: &Record,
    ) -> Result<(), OracleSessionStoreError> {
        let merge = format!(
            r#"
            MERGE INTO {} t
            USING (select :1 as myid, :2 as data, :3 as expiry_data from dual) s
               ON (t.myid = s.myid)
             WHEN MATCHED THEN
               UPDATE SET
                 t.data = s.data,
                 t.expiry_data = s.expiry_data
             WHEN NOT MATCHED THEN
               INSERT (t.myid, t.data, t.expiry_data)
               VALUES (s.myid, s.data, s.expiry_data)
            "#,
            self.table_name
        );

        let id = record.id.to_string();
        let data = rmp_serde::to_vec(&record)?;
        let expiry_date = record.expiry_date.to_utc().unix_timestamp();
        let tx = conn.clone();
        let res: Result<(), OracleSessionStoreError> = tokio::task::spawn_blocking(move || {
            tx.execute(merge.as_str(), &[&id, &data, &expiry_date])?;
            tx.commit()?;
            Ok(())
        })
        .await?;
        res
    }
}

/// An error type for SQLx stores.
#[derive(thiserror::Error, Debug)]
pub enum OracleSessionStoreError {
    #[error("Logic error {0} ")]
    Logic(String),

    #[error(transparent)]
    Oracle(#[from] bb8_oracle::oracle::Error),

    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    #[error(transparent)]
    RunError(#[from] bb8::RunError<bb8_oracle::Error>),

    /// A variant to map `rmp_serde` encode errors.
    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),

    /// A variant to map `rmp_serde` decode errors.
    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),
}

impl From<OracleSessionStoreError> for session_store::Error {
    fn from(err: OracleSessionStoreError) -> Self {
        match err {
            OracleSessionStoreError::Oracle(inner) => {
                error!("Error {:?}", inner);
                session_store::Error::Backend(inner.to_string())
            }
            OracleSessionStoreError::RunError(inner) => {
                error!("Error {:?}", inner);
                session_store::Error::Backend(inner.to_string())
            }
            OracleSessionStoreError::Decode(inner) => {
                error!("Error {:?}", inner);
                session_store::Error::Decode(inner.to_string())
            }
            OracleSessionStoreError::Encode(inner) => {
                error!("Error {:?}", inner);
                session_store::Error::Encode(inner.to_string())
            }
            OracleSessionStoreError::Join(inner) => {
                error!("Error {:?}", inner);
                session_store::Error::Backend(inner.to_string())
            }
            OracleSessionStoreError::Logic(inner) => {
                error!("Error {:?}", inner);
                session_store::Error::Backend(inner.to_string())
            }
        }
    }
}

#[async_trait]
impl ExpiredDeletion for OracleStore {
    async fn delete_expired(&self) -> session_store::Result<()> {
        let action = format!(
            r#"
            delete from {}
            where expiry_data < :3
            "#,
            self.table_name
        );
        let now = OffsetDateTime::now_utc().to_utc().unix_timestamp();
        let tx = self
            .pool
            .get()
            .await
            .map_err(OracleSessionStoreError::RunError)?
            .clone();
        let res: Result<(), OracleSessionStoreError> = tokio::task::spawn_blocking(move || {
            tx.execute(action.as_str(), &[&now])?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(OracleSessionStoreError::Join)?;
        res.map_err(|e| e.into())
    }
}

#[async_trait]
impl SessionStore for OracleStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        let mut tx = self
            .pool
            .get()
            .await
            .map_err(OracleSessionStoreError::RunError)?;

        let mut count = 0;
        while self.id_exists(&mut tx, &record.id).await? {
            count = count + 1;
            if count > 5 {
                Err(OracleSessionStoreError::Logic(
                    "count for id check exeeds limit".to_string(),
                ))?;
            }
            record.id = Id::default();
        }

        self.save_with_conn(&mut tx, record).await?;

        //tx.commit().await.map_err(SqlxStoreError::Sqlx)?;

        Ok(())
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(OracleSessionStoreError::RunError)?;
        self.save_with_conn(&mut conn, record).await?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let query = format!(
            r#"
            select data from {}
            where myid = :1 and expiry_data > :2
            "#,
            self.table_name
        );
        let tx = self
            .pool
            .get()
            .await
            .map_err(OracleSessionStoreError::RunError)?
            .clone();
        let id = session_id.to_string();
        let now = OffsetDateTime::now_utc().to_utc().unix_timestamp();
        let record_value = tokio::task::spawn_blocking(move || {
            let mut res = tx.query(query.as_str(), &[&id, &now])?;
            match res.next() {
                Some(Ok(row)) => {
                    let data: Vec<u8> = row.get(0)?;
                    Ok(Some(data))
                }
                Some(Err(e)) => Err(OracleSessionStoreError::Oracle(e)),
                None => Ok(None),
            }
        })
        .await
        .map_err(OracleSessionStoreError::Join)?;
        let record_value = record_value?;

        if let Some(data) = record_value {
            Ok(Some(
                rmp_serde::from_slice(&data).map_err(OracleSessionStoreError::Decode)?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let action = format!(r#"delete from {} where id = :1"#, self.table_name);
        let id = session_id.to_string();
        let tx = self
            .pool
            .get()
            .await
            .map_err(OracleSessionStoreError::RunError)?
            .clone();
        let res: Result<(), OracleSessionStoreError> = tokio::task::spawn_blocking(move || {
            tx.execute(action.as_str(), &[&id])?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(OracleSessionStoreError::Join)?;
        res.map_err(|e| e.into())
    }
}

/// A valid OracleSQL identifier must start with a letter or underscore
/// (including letters with diacritical marks and non-Latin letters). Subsequent
/// characters in an identifier or key word can be letters, underscores, digits
/// (0-9), or dollar signs ($). See https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS for details.
fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .next()
            .map(|c| c.is_alphabetic() || c == '_')
            .unwrap_or_default()
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        //let result = add(2, 2);
        //assert_eq!(result, 4);
    }
}
