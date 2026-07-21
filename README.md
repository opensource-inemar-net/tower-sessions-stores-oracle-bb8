This is used to add a tower session store using oracle databases.

Unfortunately it requirs that OCI is implemented on the server or in the container image.

This is the way to create a tpwer session store.

    let database_url = std::option_env!("ORACLE_URL").expect("Missing ORACLE_URL.");
    let database_user = std::option_env!("ORACLE_USER").expect("Missing ORACLE_USER.");
    let database_password = std::option_env!("ORACLE_PASSWORD").expect("Missing ORACLE_PASSWORD.");

    println!("Connecting to Oracle database at: {}", database_url);
    let manager = OracleConnectionManager::new(database_user, database_password, database_url);

    let pool = bb8::Pool::builder()
        .max_size(8)
        .build(manager)
        .await
        .unwrap();

    let session_store = OracleStore::new(pool).with_table_name("sessions")?;
    session_store.migrate().await?;

session_store migrate will create the necessary table if it is missing.

To delete expired sessions a regual task is necesary
 let deletion_task = tokio::task::spawn(
        session_store
            .clone()
            .continuously_delete_expired(tokio::time::Duration::from_secs(60)),
    );

  

To create a session  layer:

  let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(10)));
