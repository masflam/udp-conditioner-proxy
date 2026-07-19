use std::{
	collections::HashMap,
	net::SocketAddr,
	sync::{Arc, Mutex as StdMutex},
	time::{Duration, Instant},
};

use tokio::{net::UdpSocket, sync::watch, task::JoinHandle};

const BUFFER_SIZE: usize = 4096;

// target = UPSTREAM
struct UpstreamPacket {
	data: [u8; BUFFER_SIZE],
	len: usize,
	recv_time: Instant,
}

// target = some downstream
struct DownstreamPacket {
	data: [u8; BUFFER_SIZE],
	len: usize,
	target: SocketAddr,
	recv_time: Instant,
}

/// Parameters needed to construct the proxy engine. Unlike `LiveConfig`, these
/// require a rebind of sockets to change, so they are fixed for the lifetime
/// of a single `start_engine` call.
pub struct EngineParams {
	pub bind_addr: SocketAddr,
	pub upstream: SocketAddr,
}

/// Parameters that can be updated live while the proxy is running.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LiveConfig {
	pub delay_up: Duration,
	pub delay_down: Duration,
	pub max_jitter: Duration,
	pub drop_probability: f64,
	pub limit_up: f32,
	pub limit_down: f32,
}

type TaskList = Arc<StdMutex<Vec<JoinHandle<()>>>>;

/// Handle to a running proxy engine. Drop-free: call `stop` to tear it down.
pub struct EngineHandle {
	pub live_config_tx: watch::Sender<LiveConfig>,
	tasks: TaskList,
}

impl EngineHandle {
	pub async fn stop(self) {
		loop {
			let batch: Vec<_> = {
				let mut guard = self.tasks.lock().unwrap();
				std::mem::take(&mut *guard)
			};
			if batch.is_empty() {
				break;
			}
			for handle in batch {
				handle.abort();
			}
			tokio::task::yield_now().await;
		}
	}
}

pub async fn start_engine(
	params: EngineParams,
	initial_config: LiveConfig,
) -> std::io::Result<EngineHandle> {
	let proxy_bind_addr = SocketAddr::new(params.bind_addr.ip(), 0); // any port on the same ip
	let sock = Arc::new(UdpSocket::bind(params.bind_addr).await?);

	let (downstream_tx, downstream_rx) = flume::unbounded::<DownstreamPacket>();
	let (live_config_tx, live_config_rx) = watch::channel(initial_config);
	let tasks: TaskList = Arc::new(StdMutex::new(Vec::new()));

	let receiver_handle = tokio::spawn(run_downstream_receiver(
		sock.clone(),
		proxy_bind_addr,
		params.upstream,
		downstream_tx,
		live_config_rx.clone(),
		tasks.clone(),
	));
	tasks.lock().unwrap().push(receiver_handle);

	let sender_handle = tokio::spawn(run_downstream_sender(
		sock.clone(),
		downstream_rx,
		live_config_rx.clone(),
	));
	tasks.lock().unwrap().push(sender_handle);

	Ok(EngineHandle {
		live_config_tx,
		tasks,
	})
}

async fn run_downstream_receiver(
	sock: Arc<UdpSocket>,
	proxy_bind_addr: SocketAddr,
	upstream: SocketAddr,
	downstream_tx: flume::Sender<DownstreamPacket>,
	live_config_rx: watch::Receiver<LiveConfig>,
	tasks: TaskList,
) {
	let mut upstream_senders = HashMap::new();
	let mut upstream_buckets: HashMap<SocketAddr, Bucket> = HashMap::new();
	let mut buf = [0; BUFFER_SIZE];
	while let Ok((len, from_addr)) = sock.recv_from(&mut buf).await {
		let recv_time = Instant::now();
		let cfg = *live_config_rx.borrow();

		// check bandwidth limit
		let bucket = upstream_buckets
			.entry(from_addr)
			.or_insert_with(|| Bucket::new_full(cfg.limit_up, cfg.limit_up));
		bucket.set_limit(cfg.limit_up);
		if !bucket.try_reserve_at(len as f32, recv_time) {
			continue;
		}

		// check random loss
		if rand::random_bool(cfg.drop_probability) {
			continue;
		}

		// get or spawn upstream sender
		let upstream_tx = match upstream_senders.get(&from_addr).cloned() {
			Some(upstream_tx) => upstream_tx,
			None => {
				eprintln!("-> {} <-> {}", from_addr, upstream);
				let upstream_tx = match spawn_upstream_connection(
					proxy_bind_addr,
					upstream,
					from_addr,
					downstream_tx.clone(),
					live_config_rx.clone(),
					tasks.clone(),
				)
				.await
				{
					Ok(upstream_tx) => upstream_tx,
					Err(err) => {
						eprintln!("failed to spawn upstream connection for {}: {}", from_addr, err);
						continue;
					}
				};
				upstream_senders.insert(from_addr, upstream_tx.clone());
				upstream_tx
			}
		};

		let _ = upstream_tx
			.send_async(UpstreamPacket {
				data: buf,
				len,
				recv_time,
			})
			.await;
	}
}

async fn run_downstream_sender(
	sock: Arc<UdpSocket>,
	downstream_rx: flume::Receiver<DownstreamPacket>,
	live_config_rx: watch::Receiver<LiveConfig>,
) {
	while let Ok(DownstreamPacket {
		data,
		len,
		target,
		recv_time,
	}) = downstream_rx.recv_async().await
	{
		let cfg = *live_config_rx.borrow();
		tokio::time::sleep_until((recv_time + cfg.delay_down + random_duration(cfg.max_jitter)).into())
			.await;
		if sock.send_to(&data[..len], target).await.is_err() {
			eprintln!("failed to send to {}", target);
			break;
		}
	}
}

// returns upstream sender
async fn spawn_upstream_connection(
	proxy_bind_addr: SocketAddr,
	upstream: SocketAddr,
	downstream: SocketAddr,
	downstream_tx: flume::Sender<DownstreamPacket>,
	live_config_rx: watch::Receiver<LiveConfig>,
	tasks: TaskList,
) -> std::io::Result<flume::Sender<UpstreamPacket>> {
	let sock = Arc::new(UdpSocket::bind(proxy_bind_addr).await?);
	let (upstream_tx, upstream_rx) = flume::unbounded::<UpstreamPacket>();
	let initial_limit_down = live_config_rx.borrow().limit_down;

	// spawn upstream receiver
	let receiver_handle = tokio::spawn({
		let mut bucket = Bucket::new_full(initial_limit_down, initial_limit_down);
		let sock = sock.clone();
		let live_config_rx = live_config_rx.clone();
		async move {
			let mut buf = [0; BUFFER_SIZE];
			while let Ok((len, from_addr)) = sock.recv_from(&mut buf).await {
				let recv_time = Instant::now();
				if from_addr != upstream {
					eprintln!(
						"received data from unexpected address: {} (expected: {})",
						from_addr, upstream
					);
					continue;
				}

				let cfg = *live_config_rx.borrow();

				// check bandwidth limit
				bucket.set_limit(cfg.limit_down);
				if !bucket.try_reserve_at(len as f32, recv_time) {
					continue;
				}

				// check random loss
				if rand::random_bool(cfg.drop_probability) {
					continue;
				}

				let _ = downstream_tx
					.send_async(DownstreamPacket {
						data: buf,
						len,
						target: downstream,
						recv_time,
					})
					.await;
			}
		}
	});

	// spawn upstream sender
	let sender_handle = tokio::spawn({
		let live_config_rx = live_config_rx.clone();
		async move {
			while let Ok(UpstreamPacket {
				data,
				len,
				recv_time,
			}) = upstream_rx.recv_async().await
			{
				let cfg = *live_config_rx.borrow();
				tokio::time::sleep_until((recv_time + cfg.delay_up + random_duration(cfg.max_jitter)).into())
					.await;
				if sock.send_to(&data[..len], upstream).await.is_err() {
					eprintln!("failed to send to {}", upstream);
					break;
				}
			}
		}
	});

	{
		let mut guard = tasks.lock().unwrap();
		guard.push(receiver_handle);
		guard.push(sender_handle);
	}

	Ok(upstream_tx)
}

fn random_duration(max_duration: Duration) -> Duration {
	let max_micros = max_duration.as_micros() as u64;
	if max_micros == 0 {
		return Duration::from_micros(0);
	}
	let micros = rand::random_range(0..max_micros);
	Duration::from_micros(micros)
}

struct Bucket {
	current_val: f32,
	current_time: Instant,
	refill_per_second: f32,
	max_val: f32,
}

impl Bucket {
	pub fn new_full(max_val: f32, refill_per_second: f32) -> Self {
		Self {
			max_val,
			current_val: max_val,
			current_time: Instant::now(),
			refill_per_second,
		}
	}

	/// React to a live bandwidth-limit change. If the limit was lowered,
	/// clamp the current fill level down immediately so the next packet
	/// can't burst above the new limit. If raised, let the bucket earn its
	/// way up to the new capacity via `refill` rather than granting a free
	/// burst.
	fn set_limit(&mut self, new_limit: f32) {
		if new_limit == self.max_val {
			return;
		}
		self.max_val = new_limit;
		self.refill_per_second = new_limit;
		self.current_val = self.current_val.min(self.max_val);
	}

	fn refill(&mut self, time: Instant) {
		let elapsed = time.saturating_duration_since(self.current_time);
		self.current_time = time;
		self.current_val = (self.current_val + elapsed.as_secs_f32() * self.refill_per_second)
			.clamp(0.0, self.max_val);
	}

	fn try_reserve_at(&mut self, v: f32, time: Instant) -> bool {
		self.refill(time);
		if self.current_val >= v {
			self.current_val -= v;
			true
		} else {
			false
		}
	}
}
