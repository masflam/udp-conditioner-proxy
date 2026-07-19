mod app;
mod proxy;

use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
	/// Change the bind address
	#[arg(
		short,
		long,
		value_name = "BIND_ADDRESS",
		default_value = "127.0.0.1:7777"
	)]
	from: SocketAddr,

	/// Upstream address (where to proxy to)
	#[arg(short, long, value_name = "ADDRESS", default_value = "127.0.0.1:9000")]
	to: SocketAddr,

	/// Delay to add to each packet
	#[arg(short, long, value_name = "MILLIS", default_value = "0")]
	delay: u64,

	/// Delay to add to each packet going from client to server (upstream)
	#[arg(long, value_name = "MILLIS", default_value = "0")]
	delay_up: u64,

	/// Delay to add to each packet going from server to client (downstream)
	#[arg(long, value_name = "MILLIS", default_value = "0")]
	delay_down: u64,

	/// Jitter to add to each packet
	#[arg(long, value_name = "MILLIS", default_value = "0")]
	jitter: u64,

	/// Loss rate
	#[arg(short, long, value_name = "DROP_PROBABILITY", default_value = "0")]
	loss: f64,

	/// Limit bandwidth
	#[arg(
		short,
		long,
		value_name = "BYTES_PER_SECOND",
		default_value = "1073741824"
	)]
	bandwidth_limit: f32,

	/// Limit transfer rate from client to server (upstream)
	#[arg(long, value_name = "BYTES_PER_SECOND", default_value = "1073741824")]
	bandwidth_limit_up: f32,

	/// Limit transfer rate from server to client (downstream)
	#[arg(long, value_name = "BYTES_PER_SECOND", default_value = "1073741824")]
	bandwidth_limit_down: f32,
}

fn main() -> eframe::Result<()> {
	let args = Cli::parse();

	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.expect("failed to build tokio runtime");

	let app = app::ProxyApp::new(args, runtime);

	eframe::run_native(
		"UDP Conditioner Proxy",
		eframe::NativeOptions::default(),
		Box::new(|_cc| Ok(Box::new(app))),
	)
}
