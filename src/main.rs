use async_std::prelude::*;
use async_std::stream;
use async_trait::async_trait;
use chrono::prelude::*;
use clap::Parser;
use futures::future::join_all;
use std::fs;
use std::{
    io::{Error, ErrorKind},
    time::Duration,
};
use time::OffsetDateTime;
use yahoo_finance_api as yahoo;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long)]
    symbols: Option<String>,
    #[clap(short, long)]
    from: String,
    #[clap(long)]
    file: Option<String>,
}

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait AsyncStockSignal {
    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

///
/// Find the maximum in a series of f64
///
struct MaxPrice {}

#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        }
    }
}

///
/// Find the minimum in a series of f64
///
struct MinPrice {}

#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

/// PriceDifference

struct PriceDifference {}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    type SignalType = (f64, f64);

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            // unwrap is safe here even if first == last
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

struct WindowedSMA {
    window_size: usize,
}

#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() && self.window_size > 1 {
            Some(
                series
                    .windows(self.window_size)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();

    let b = OffsetDateTime::from_unix_timestamp(beginning.timestamp()).unwrap();
    let e = OffsetDateTime::from_unix_timestamp(end.timestamp()).unwrap();

    let response = provider
        .get_quote_history(symbol, b, e)
        .await
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

fn read_symbols_from_file(filename: &str) -> Option<Vec<String>> {
    let content = fs::read_to_string(filename).ok().map(|s| {
        s.split(',')
            .map(|s| String::from(s.trim()))
            .collect::<Vec<String>>()
    });

    content
}

///
/// Convenience function that chains together the entire processing chain.
///
async fn handle_symbol_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Option<Vec<f64>> {
    let closes = fetch_closing_data(symbol, beginning, end).await.ok()?;
    if !closes.is_empty() {
        let diff = PriceDifference {};
        let min = MinPrice {};
        let max = MaxPrice {};
        let sma = WindowedSMA { window_size: 30 };

        let period_max: f64 = max.calculate(&closes).await?;
        let period_min: f64 = min.calculate(&closes).await?;

        let last_price = *closes.last()?;
        let (_, pct_change) = diff.calculate(&closes).await?;
        let sma = sma.calculate(&closes).await?;

        // a simple way to output CSV data
        println!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
            beginning.to_rfc3339(),
            symbol,
            last_price,
            pct_change * 100.0,
            period_min,
            period_max,
            sma.last().unwrap_or(&0.0)
        );
    }
    Some(closes)
}

async fn fetch_all_symbol_data(
    symbols: &Vec<String>,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Vec<Option<Vec<f64>>> {
    let handles = symbols
        .iter()
        .map(|symbol| handle_symbol_data(symbol, beginning, end));
    join_all(handles).await
}

#[async_std::main]
async fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let to = Utc::now();
    let file: Option<String> = opts.file;

    let symbols: Vec<String> = file
        .map(|f| read_symbols_from_file(f.as_str()))
        .flatten()
        .or({
            opts.symbols
                .map(|s| s.split(',').map(String::from).collect::<Vec<String>>())
        })
        .expect("Either pass --file or --symbols to pass the symbols to fetch.");

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");

    // one initial run
    fetch_all_symbol_data(&symbols, &from, &to).await;
    // Run every 30 seconds
    let mut interval = stream::interval(Duration::from_secs(30));
    while let Some(_) = interval.next().await {
        fetch_all_symbol_data(&symbols, &from, &to).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[async_std::test]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[async_std::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[async_std::test]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[async_std::test]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }
}
