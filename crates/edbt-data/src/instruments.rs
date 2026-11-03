//! Point-in-time instrument metadata (`configs/instruments.toml`, DQ-07) and
//! price/quantity conversion to integer ticks and lots (DQ-05).

use std::path::Path;

use serde::Deserialize;

use crate::{DataError, Result};

/// Relative tolerance of DQ-05: `|x/unit - round(x/unit)| <= 1e-6`.
pub const GRID_TOLERANCE: f64 = 1e-6;

/// Convert `value` to an integer number of `unit`s, or `None` if it is off
/// the grid by more than [`GRID_TOLERANCE`] or out of range (DQ-05).
#[must_use]
pub fn to_units(value: f64, unit: f64) -> Option<i64> {
    let x = value / unit;
    let r = x.round();
    if !r.is_finite() || (x - r).abs() > GRID_TOLERANCE || r.abs() > 9.0e18 {
        return None;
    }
    // In range and integral by the checks above.
    #[allow(clippy::cast_possible_truncation)]
    Some(r as i64)
}

/// Contract kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractType {
    /// USDT-margined linear perpetual.
    LinearPerp,
    /// Cash equity (L3 replay).
    Equity,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInstrument {
    venue: String,
    symbol: String,
    tick_size: f64,
    lot_size: f64,
    contract_type: ContractType,
    maker_bps: f64,
    taker_bps: f64,
    effective_from: String,
    #[serde(default)]
    min_notional: Option<f64>,
    #[serde(default)]
    contract_mult: Option<f64>,
    #[serde(default)]
    l3_price_unit: Option<f64>,
    #[serde(default)]
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    #[serde(default)]
    instrument: Vec<RawInstrument>,
}

/// One instrument version, effective from `effective_from` (UTC midnight).
#[derive(Debug, Clone, PartialEq)]
pub struct Instrument {
    /// Venue id.
    pub venue: String,
    /// Venue-native symbol.
    pub symbol: String,
    /// Price increment, quote currency.
    pub tick_size: f64,
    /// Quantity increment, base units.
    pub lot_size: f64,
    /// Contract kind.
    pub contract_type: ContractType,
    /// Maker fee in micro-basis-points (bps x 1e6); negative is a rebate.
    pub maker_ubps: i64,
    /// Taker fee in micro-basis-points.
    pub taker_ubps: i64,
    /// Start of validity, ns UTC.
    pub effective_from: i64,
    /// Minimum order notional, quote currency, when the venue has one.
    pub min_notional: Option<f64>,
    /// Contract multiplier (1 for linear perps and equities).
    pub contract_mult: f64,
    /// Value of one lot at a price of one tick, in nano-quote (FR-16).
    pub nq_per_tick_lot: i128,
    /// `book_l3.price_ticks` units per engine tick (1 unless the producer
    /// uses a finer price unit, e.g. XNAS 1e-4 USD units with a 0.01 tick).
    pub l3_units_per_tick: i64,
    /// Where the values came from (venue endpoint and date), if recorded.
    pub source: Option<String>,
}

impl Instrument {
    /// Ticks for a quote-currency price (DQ-05).
    #[must_use]
    pub fn price_to_ticks(&self, price: f64) -> Option<i64> {
        to_units(price, self.tick_size)
    }

    /// Lots for a base-unit quantity (DQ-05).
    #[must_use]
    pub fn qty_to_lots(&self, qty: f64) -> Option<i64> {
        to_units(qty, self.lot_size)
    }

    /// Engine ticks for an L3 producer price, if it lies on the tick grid.
    #[must_use]
    pub fn l3_price_to_ticks(&self, price_units: i64) -> Option<i64> {
        (price_units % self.l3_units_per_tick == 0).then(|| price_units / self.l3_units_per_tick)
    }

    /// Quote-currency price of `ticks` (derived value for reports and Python).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn ticks_to_price(&self, ticks: i64) -> f64 {
        ticks as f64 * self.tick_size
    }

    /// Base-unit quantity of `lots`.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn lots_to_qty(&self, lots: i64) -> f64 {
        lots as f64 * self.lot_size
    }
}

fn bps_to_ubps(bps: f64, what: &str, sym: &str) -> Result<i64> {
    to_units(bps, 1e-6)
        .ok_or_else(|| DataError::Instrument(format!("{sym}: {what} {bps} is not a multiple of 1e-6 bps")))
}

impl TryFrom<RawInstrument> for Instrument {
    type Error = DataError;

    fn try_from(r: RawInstrument) -> Result<Self> {
        let sym = format!("{}/{}", r.venue, r.symbol);
        if !(r.tick_size > 0.0 && r.lot_size > 0.0) {
            return Err(DataError::Instrument(format!("{sym}: tick_size and lot_size must be positive")));
        }
        let mult = r.contract_mult.unwrap_or(1.0);
        let nq = to_units(r.tick_size * r.lot_size * mult * 1e9, 1.0).filter(|v| *v > 0).ok_or_else(|| {
            DataError::Instrument(format!(
                "{sym}: tick_size x lot_size x contract_mult is not a whole number of nano-quote (FR-16)"
            ))
        })?;
        let l3_units = match r.l3_price_unit {
            None => 1,
            Some(u) => to_units(r.tick_size, u)
                .filter(|v| *v > 0)
                .ok_or_else(|| DataError::Instrument(format!("{sym}: tick_size is not a multiple of l3_price_unit")))?,
        };
        let effective_from = edbt_core::time::parse_rfc3339(&r.effective_from)
            .map_err(|e| DataError::Instrument(format!("{sym}: {e}")))?;
        Ok(Self {
            maker_ubps: bps_to_ubps(r.maker_bps, "maker_bps", &sym)?,
            taker_ubps: bps_to_ubps(r.taker_bps, "taker_bps", &sym)?,
            venue: r.venue,
            symbol: r.symbol,
            tick_size: r.tick_size,
            lot_size: r.lot_size,
            contract_type: r.contract_type,
            effective_from,
            min_notional: r.min_notional,
            contract_mult: mult,
            nq_per_tick_lot: i128::from(nq),
            l3_units_per_tick: l3_units,
            source: r.source,
        })
    }
}

/// All instrument versions from one file, sorted by venue, symbol and
/// effective date.
#[derive(Debug, Clone, Default)]
pub struct InstrumentTable {
    items: Vec<Instrument>,
}

impl InstrumentTable {
    /// Parse TOML text with `[[instrument]]` tables.
    ///
    /// # Errors
    /// [`DataError::Instrument`] on parse errors or invalid values.
    pub fn from_toml_str(text: &str) -> Result<Self> {
        let raw: RawFile = toml::from_str(text).map_err(|e| DataError::Instrument(e.to_string()))?;
        let mut items = raw.instrument.into_iter().map(Instrument::try_from).collect::<Result<Vec<_>>>()?;
        items.sort_by(|a, b| {
            (a.venue.as_str(), a.symbol.as_str(), a.effective_from).cmp(&(
                b.venue.as_str(),
                b.symbol.as_str(),
                b.effective_from,
            ))
        });
        Ok(Self { items })
    }

    /// Load a TOML file.
    ///
    /// # Errors
    /// I/O or parse errors.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| DataError::io(path, e))?;
        Self::from_toml_str(&text)
    }

    /// The version in force at `ts` (latest `effective_from <= ts`, DQ-07).
    #[must_use]
    pub fn get(&self, venue: &str, symbol: &str, ts: i64) -> Option<&Instrument> {
        self.items.iter().rev().find(|i| i.venue == venue && i.symbol == symbol && i.effective_from <= ts)
    }

    /// Every version of one instrument, oldest first.
    pub fn versions<'a>(
        &'a self,
        venue: &'a str,
        symbol: &'a str,
    ) -> impl DoubleEndedIterator<Item = &'a Instrument> + 'a {
        self.items.iter().filter(move |i| i.venue == venue && i.symbol == symbol)
    }

    /// All entries.
    #[must_use]
    pub fn all(&self) -> &[Instrument] {
        &self.items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOML: &str = r#"
[[instrument]]
venue = "binance-usdm"
symbol = "BTCUSDT"
tick_size = 0.1
lot_size = 0.001
contract_type = "linear_perp"
maker_bps = 2.0
taker_bps = 5.0
effective_from = "2026-01-01"

[[instrument]]
venue = "binance-usdm"
symbol = "BTCUSDT"
tick_size = 0.1
lot_size = 0.001
contract_type = "linear_perp"
maker_bps = -0.5
taker_bps = 4.5
effective_from = "2026-06-01"

[[instrument]]
venue = "XNAS"
symbol = "AAPL"
tick_size = 0.01
lot_size = 1
contract_type = "equity"
maker_bps = 0
taker_bps = 0.3
effective_from = "2019-01-01"
l3_price_unit = 0.0001
"#;

    #[test]
    fn point_in_time_lookup() {
        let t = InstrumentTable::from_toml_str(TOML).unwrap();
        let jan = edbt_core::time::parse_rfc3339("2026-03-01").unwrap();
        let jul = edbt_core::time::parse_rfc3339("2026-07-01").unwrap();
        assert_eq!(t.get("binance-usdm", "BTCUSDT", jan).unwrap().maker_ubps, 2_000_000);
        assert_eq!(t.get("binance-usdm", "BTCUSDT", jul).unwrap().maker_ubps, -500_000);
        assert!(t.get("binance-usdm", "BTCUSDT", 0).is_none());
        assert!(t.get("binance-usdm", "ETHUSDT", jul).is_none());
        let btc = t.get("binance-usdm", "BTCUSDT", jul).unwrap();
        assert_eq!(btc.nq_per_tick_lot, 100_000);
        assert_eq!(btc.price_to_ticks(76_903.9), Some(769_039));
        assert_eq!(btc.qty_to_lots(5.543), Some(5_543));
        assert_eq!(btc.price_to_ticks(76_903.95), None);
        let aapl = t.get("XNAS", "AAPL", jul).unwrap();
        assert_eq!(aapl.l3_units_per_tick, 100);
        assert_eq!(aapl.l3_price_to_ticks(1_293_300), Some(12_933));
        assert_eq!(aapl.l3_price_to_ticks(1_293_350), None);
        assert_eq!(aapl.nq_per_tick_lot, 10_000_000);
    }

    #[test]
    fn rejects_fractional_nano_quote() {
        let bad = TOML.replace("lot_size = 1\n", "lot_size = 0.0000000001\n");
        assert!(InstrumentTable::from_toml_str(&bad).is_err());
        let neg = TOML.replace("tick_size = 0.01", "tick_size = -0.01");
        assert!(InstrumentTable::from_toml_str(&neg).is_err());
    }

    #[test]
    fn conversion_tolerance() {
        assert_eq!(to_units(0.30000000000000004, 0.1), Some(3));
        assert_eq!(to_units(0.35, 0.1), None);
        assert_eq!(to_units(f64::NAN, 0.1), None);
    }
}
