//! AC-W-12: config load and the request layer both refuse mainnet hosts and
//! any non-allowlisted host; the secret never appears in Debug output.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use edbt_live::guard::{Credentials, ENV_API_KEY, ENV_API_SECRET, HostGuard, LiveGuardError, is_mainnet_host};

const ALLOWED: &[&str] = &[
    "testnet.binancefuture.com",
    "stream.binancefuture.com",
    "fstream.binancefuture.com",
    "demo-fapi.binance.com",
    "demo-fstream.binance.com",
];

const MAINNET: &[&str] = &["fapi.binance.com", "fstream.binance.com", "ws-fapi.binance.com", "api.binance.com"];

#[test]
fn config_load_rejects_mainnet_hosts() {
    for host in MAINNET {
        let mut hosts = ALLOWED.to_vec();
        hosts.push(host);
        assert_eq!(HostGuard::new(&hosts), Err(LiveGuardError::Mainnet((*host).to_owned())));
        // Case and trailing whitespace do not help.
        let upper = format!(" {} ", host.to_uppercase());
        assert!(matches!(HostGuard::new(&[upper]), Err(LiveGuardError::Mainnet(_))));
    }
    assert!(matches!(HostGuard::new(&["other.binance.com"]), Err(LiveGuardError::Mainnet(_))));
    assert!(matches!(HostGuard::new(&["*.binancefuture.com"]), Err(LiveGuardError::BadEntry(_))));
    assert!(matches!(HostGuard::new(&[""]), Err(LiveGuardError::BadEntry(_))));
}

#[test]
fn request_layer_rejects_mainnet_and_unknown_hosts() {
    let guard = HostGuard::new(ALLOWED).unwrap();
    for host in MAINNET {
        for scheme in ["https", "wss"] {
            let url = format!("{scheme}://{host}/fapi/v1/time");
            assert_eq!(guard.check_url(&url), Err(LiveGuardError::Mainnet((*host).to_owned())));
        }
    }
    assert_eq!(
        guard.check_url("https://example.com/fapi/v1/time"),
        Err(LiveGuardError::NotAllowed("example.com".to_owned()))
    );
    assert!(matches!(guard.check_url("http://testnet.binancefuture.com/"), Err(LiveGuardError::BadScheme(_))));
    assert!(matches!(guard.check_url("not a url"), Err(LiveGuardError::BadUrl(_))));
    // Userinfo tricks resolve to the real host.
    assert!(matches!(
        guard.check_url("https://testnet.binancefuture.com@fapi.binance.com/"),
        Err(LiveGuardError::Mainnet(_))
    ));
}

#[test]
fn allowlisted_testnet_hosts_pass() {
    let guard = HostGuard::new(ALLOWED).unwrap();
    for host in ALLOWED {
        let url = guard.check_url(&format!("https://{host}/fapi/v1/time")).unwrap();
        assert_eq!(url.host_str(), Some(*host));
    }
    assert!(guard.check_url("wss://stream.binancefuture.com/ws/btcusdt@depth@100ms").is_ok());
    assert_eq!(guard.hosts().count(), ALLOWED.len());
}

#[test]
fn mainnet_detection() {
    assert!(is_mainnet_host("FAPI.binance.com."));
    assert!(is_mainnet_host("binance.com"));
    assert!(!is_mainnet_host("demo-fapi.binance.com"));
    assert!(!is_mainnet_host("testnet.binancefuture.com"));
}

#[test]
fn secret_never_in_debug_or_display() {
    let secret = "s3cr3t-value-that-must-not-leak";
    let creds = Credentials::from_lookup(|k| match k {
        ENV_API_KEY => Some("public-key".to_owned()),
        ENV_API_SECRET => Some(secret.to_owned()),
        _ => panic!("unexpected variable {k}"),
    })
    .unwrap();
    let dbg = format!("{creds:?} {:?} {}", creds.secret, creds.secret);
    assert!(!dbg.contains(secret), "{dbg}");
    assert!(dbg.contains("public-key"));
    assert_eq!(creds.secret.expose(), secret);
}

#[test]
fn credentials_only_from_testnet_variables() {
    let none = Credentials::from_lookup(|k| match k {
        ENV_API_KEY => Some("k".to_owned()),
        ENV_API_SECRET => Some("  ".to_owned()),
        _ => panic!("unexpected variable {k}"),
    });
    assert!(none.is_none());
    let none = Credentials::from_lookup(|_| None);
    assert!(none.is_none());
}

#[test]
fn plain_schemes_only_for_an_allowlisted_loopback_mock() {
    let guard = HostGuard::new(&["127.0.0.1", "demo-fapi.binance.com"]).unwrap();
    assert!(guard.check_url("http://127.0.0.1:8080/fapi/v1/time").is_ok());
    assert!(guard.check_url("ws://127.0.0.1:9000/public/stream").is_ok());
    assert!(matches!(guard.check_url("http://demo-fapi.binance.com/"), Err(LiveGuardError::BadScheme(_))));
    assert!(matches!(guard.check_url("http://localhost:8080/"), Err(LiveGuardError::NotAllowed(_))));
    assert!(matches!(guard.check_url("ws://fstream.binance.com/ws"), Err(LiveGuardError::Mainnet(_))));
}

#[test]
fn deribit_mainnet_is_refused_and_only_the_testnet_host_allowed() {
    for h in ["www.deribit.com", "deribit.com", "history.deribit.com", "streams.deribit.com", "WWW.DERIBIT.COM."] {
        assert!(is_mainnet_host(h), "{h}");
        assert!(
            matches!(HostGuard::new(&[h]), Err(LiveGuardError::Mainnet(_)) | Err(LiveGuardError::BadEntry(_))),
            "{h}"
        );
    }
    assert!(!is_mainnet_host("test.deribit.com"));
    let guard = HostGuard::new(&["test.deribit.com"]).unwrap();
    assert!(guard.check_url("wss://test.deribit.com/ws/api/v2").is_ok());
    assert!(matches!(guard.check_url("wss://www.deribit.com/ws/api/v2"), Err(LiveGuardError::Mainnet(_))));
    assert!(matches!(guard.check_url("https://deribit.com/api/v2/public/get_time"), Err(LiveGuardError::Mainnet(_))));
    let creds = edbt_live::guard::Credentials::deribit_from_lookup(|k| match k {
        "DERIBIT_TESTNET_CLIENT_ID" => Some("id".to_owned()),
        "DERIBIT_TESTNET_CLIENT_SECRET" => Some("very-secret-value".to_owned()),
        _ => panic!("asked for {k}"),
    })
    .unwrap();
    assert!(!format!("{creds:?}").contains("very-secret-value"));
}
