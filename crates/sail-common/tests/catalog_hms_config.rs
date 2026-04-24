#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use sail_common::config::CatalogType;

#[test]
fn test_deserialize_hive_metastore_catalog_type() {
    let catalog: CatalogType = serde_json::from_str(
        r#"{
            "type": "hive_metastore",
            "name": "hms",
            "uris": ["127.0.0.1:9083"],
            "thrift_transport": "framed",
            "auth": "kerberos",
            "kerberos_service_principal": "hive-metastore/_HOST@EXAMPLE.COM",
            "min_sasl_qop": "auth_int",
            "connect_timeout_secs": 9
        }"#,
    )
    .expect("catalog type should deserialize");

    match catalog {
        CatalogType::HiveMetastore {
            name,
            uris,
            thrift_transport,
            auth,
            kerberos_service_principal,
            min_sasl_qop,
            connect_timeout_secs,
        } => {
            assert_eq!(name, "hms");
            assert_eq!(uris, vec!["127.0.0.1:9083"]);
            assert_eq!(thrift_transport.as_deref(), Some("framed"));
            assert_eq!(auth.as_deref(), Some("kerberos"));
            assert_eq!(
                kerberos_service_principal.as_deref(),
                Some("hive-metastore/_HOST@EXAMPLE.COM")
            );
            assert_eq!(min_sasl_qop.as_deref(), Some("auth_int"));
            assert_eq!(connect_timeout_secs, Some(9));
        }
        other => panic!("unexpected catalog type: {other:?}"),
    }
}

#[test]
fn test_deserialize_hms_catalog_type_alias() {
    let catalog: CatalogType = serde_json::from_str(
        r#"{
            "type": "hms",
            "name": "hms",
            "uris": ["127.0.0.1:9083"]
        }"#,
    )
    .expect("catalog type alias should deserialize");

    match catalog {
        CatalogType::HiveMetastore { name, uris, .. } => {
            assert_eq!(name, "hms");
            assert_eq!(uris, vec!["127.0.0.1:9083"]);
        }
        other => panic!("unexpected catalog type: {other:?}"),
    }
}

#[test]
fn test_deserialize_hms_catalog_defaults_to_none_auth() {
    let catalog: CatalogType = serde_json::from_str(
        r#"{
            "type": "hms",
            "name": "hms",
            "uris": ["127.0.0.1:9083"]
        }"#,
    )
    .expect("catalog type alias should deserialize");

    match catalog {
        CatalogType::HiveMetastore {
            auth,
            min_sasl_qop,
            connect_timeout_secs,
            ..
        } => {
            assert!(auth.is_none());
            assert!(min_sasl_qop.is_none());
            assert!(connect_timeout_secs.is_none());
        }
        other => panic!("unexpected catalog type: {other:?}"),
    }
}

#[test]
fn test_deserialize_hms_catalog_rejects_legacy_uri_field() {
    let error = serde_json::from_str::<CatalogType>(
        r#"{
            "type": "hms",
            "name": "hms",
            "uri": "127.0.0.1:9083"
        }"#,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unknown field"));
    assert!(error.to_string().contains("uri"));
}

#[test]
fn test_deserialize_hive_metastore_catalog_type_alias() {
    let catalog: CatalogType = serde_json::from_str(
        r#"{
            "type": "hive-metastore",
            "name": "hms",
            "uris": ["127.0.0.1:9083"]
        }"#,
    )
    .expect("catalog type alias should deserialize");

    match catalog {
        CatalogType::HiveMetastore { name, uris, .. } => {
            assert_eq!(name, "hms");
            assert_eq!(uris, vec!["127.0.0.1:9083"]);
        }
        other => panic!("unexpected catalog type: {other:?}"),
    }
}
