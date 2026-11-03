//! Hand-computed fill scenarios (AC-W-03): every `tests/fixtures/scenarios/*.toml`
//! file lists input events and the exact responses, fills and fees expected
//! for each order. The expected values are typed into the files by hand.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use common::{H, Opts};
use edbt_book::l3::L3Op;
use edbt_core::ClientOrderId;
use edbt_core::config::QueueModelKind;
use edbt_core::gateway::ResponseKind;
use edbt_core::order::{OrdType, Side, TimeInForce};

fn side(v: &toml::Value) -> Side {
    match v.as_str().unwrap() {
        "buy" | "bid" => Side::Buy,
        "sell" | "ask" => Side::Sell,
        other => panic!("bad side {other}"),
    }
}

fn int(t: &toml::Table, k: &str) -> i64 {
    t.get(k).and_then(toml::Value::as_integer).unwrap_or_else(|| panic!("missing integer {k}"))
}

fn levels(v: Option<&toml::Value>) -> Vec<(i64, i64)> {
    v.and_then(toml::Value::as_array)
        .map(|a| a.iter().map(|l| (l[0].as_integer().unwrap(), l[1].as_integer().unwrap())).collect())
        .unwrap_or_default()
}

fn render(k: &ResponseKind) -> String {
    match k {
        ResponseKind::Accepted { .. } => "accepted".into(),
        ResponseKind::Rejected { reason } => format!("rejected {reason}"),
        ResponseKind::Fill { price, qty, liquidity, fee, .. } => {
            format!("fill {price} {qty} {} {fee}", liquidity.as_str())
        }
        ResponseKind::Canceled => "canceled".into(),
        ResponseKind::CancelRejected { reason } => format!("cancel_rejected {reason}"),
        ResponseKind::Expired { reason } => format!("expired {reason}"),
    }
}

fn run(path: &Path) -> usize {
    let doc: toml::Table = toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let setup = doc.get("setup").and_then(toml::Value::as_table).cloned().unwrap_or_default();
    let mut o = Opts::default();
    if let Some(q) = setup.get("queue").and_then(toml::Value::as_str) {
        o.queue.model = match q {
            "none" => QueueModelKind::None,
            "risk_averse" => QueueModelKind::RiskAverse,
            "prob_power" => QueueModelKind::ProbPower,
            "prob_log" => QueueModelKind::ProbLog,
            "l3_fifo" => QueueModelKind::L3Fifo,
            other => panic!("bad queue {other}"),
        };
    }
    if let Some(p) = setup.get("power").and_then(toml::Value::as_float) {
        o.queue.power = p;
    }
    o.entry_ns = setup.get("entry_ns").and_then(toml::Value::as_integer).unwrap_or(0);
    o.response_ns = setup.get("response_ns").and_then(toml::Value::as_integer).unwrap_or(0);
    o.l3 = setup.get("l3").and_then(toml::Value::as_bool).unwrap_or(false);
    let mut h = H::new(o);
    let mut refs: BTreeMap<String, ClientOrderId> = BTreeMap::new();
    for e in doc["events"].as_array().unwrap() {
        let e = e.as_table().unwrap();
        let t = int(e, "t");
        match e["kind"].as_str().unwrap() {
            "snapshot" => h.snapshot(t, &levels(e.get("bids")), &levels(e.get("asks"))),
            "level" => h.level(t, side(&e["side"]), int(e, "price"), int(e, "qty")),
            "gap_level" => h.gap_level(t, side(&e["side"]), int(e, "price"), int(e, "qty")),
            "trade" => h.trade(t, i8::try_from(int(e, "side")).unwrap(), int(e, "price"), int(e, "qty")),
            "l3" => {
                let op = match e["op"].as_str().unwrap() {
                    "add" => L3Op::Add,
                    "exec" => L3Op::Exec,
                    "cancel" => L3Op::Cancel,
                    "delete" => L3Op::Delete,
                    "replace" => L3Op::Replace,
                    other => panic!("bad op {other}"),
                };
                let new_id = e.get("new_id").and_then(toml::Value::as_integer).unwrap_or(0);
                h.l3(
                    t,
                    op,
                    u64::try_from(int(e, "id")).unwrap(),
                    side(&e["side"]),
                    int(e, "price"),
                    int(e, "qty"),
                    u64::try_from(new_id).unwrap(),
                );
            }
            "order" => {
                let ord_type = if e["type"].as_str() == Some("market") { OrdType::Market } else { OrdType::Limit };
                let tif = match e["tif"].as_str().unwrap() {
                    "gtc" => TimeInForce::Gtc,
                    "ioc" => TimeInForce::Ioc,
                    "fok" => TimeInForce::Fok,
                    "gtx" => TimeInForce::Gtx,
                    other => panic!("bad tif {other}"),
                };
                let price = e.get("price").and_then(toml::Value::as_integer);
                let id = h.order(t, side(&e["side"]), ord_type, tif, price, int(e, "qty"));
                refs.insert(e["ref"].as_str().unwrap().to_owned(), id);
            }
            "cancel" => {
                let id = refs[e["ref"].as_str().unwrap()];
                h.cancel(t, id);
            }
            other => panic!("bad event kind {other}"),
        }
    }
    h.settle();
    let expected = doc["expected"].as_array().unwrap();
    for x in expected {
        let x = x.as_table().unwrap();
        let r = x["ref"].as_str().unwrap();
        let want: Vec<String> =
            x["responses"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_owned()).collect();
        let got: Vec<String> = h.responses(refs[r]).iter().map(render).collect();
        assert_eq!(got, want, "{}: order {r}", path.display());
    }
    expected.len()
}

#[test]
fn hand_computed_scenarios_pass() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/scenarios");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "toml"))
        .collect();
    files.sort();
    assert!(files.len() >= 15, "only {} scenarios", files.len());
    let checked: usize = files.iter().map(|f| run(f)).sum();
    assert!(checked >= files.len());
}
