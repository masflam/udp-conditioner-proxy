use std::time::Duration;

use eframe::egui;

use crate::{
	proxy::{self, EngineParams, LiveConfig},
	Cli,
};

pub struct ProxyApp {
	from_str: String,
	to_str: String,
	delay_ms: u64,
	delay_up_ms: u64,
	delay_down_ms: u64,
	jitter_ms: u64,
	loss: f64,
	bandwidth_limit: f32,
	bandwidth_limit_up: f32,
	bandwidth_limit_down: f32,

	runtime: tokio::runtime::Runtime,
	engine: Option<proxy::EngineHandle>,
	error: Option<String>,
	addr_parse_error: Option<String>,
}

impl ProxyApp {
	pub fn new(args: Cli, runtime: tokio::runtime::Runtime) -> Self {
		Self {
			from_str: args.from.to_string(),
			to_str: args.to.to_string(),
			delay_ms: args.delay,
			delay_up_ms: args.delay_up,
			delay_down_ms: args.delay_down,
			jitter_ms: args.jitter,
			loss: args.loss,
			bandwidth_limit: args.bandwidth_limit,
			bandwidth_limit_up: args.bandwidth_limit_up,
			bandwidth_limit_down: args.bandwidth_limit_down,
			runtime,
			engine: None,
			error: None,
			addr_parse_error: None,
		}
	}

	fn build_live_config(&self) -> LiveConfig {
		LiveConfig {
			delay_up: Duration::from_millis(self.delay_ms) + Duration::from_millis(self.delay_up_ms),
			delay_down: Duration::from_millis(self.delay_ms) + Duration::from_millis(self.delay_down_ms),
			max_jitter: Duration::from_millis(self.jitter_ms),
			drop_probability: self.loss,
			limit_up: self.bandwidth_limit.min(self.bandwidth_limit_up),
			limit_down: self.bandwidth_limit.min(self.bandwidth_limit_down),
		}
	}

	fn start_engine(&mut self) {
		self.error = None;

		let from = match self.from_str.parse() {
			Ok(addr) => addr,
			Err(err) => {
				self.addr_parse_error = Some(format!("invalid bind address: {err}"));
				return;
			}
		};
		let to = match self.to_str.parse() {
			Ok(addr) => addr,
			Err(err) => {
				self.addr_parse_error = Some(format!("invalid upstream address: {err}"));
				return;
			}
		};
		self.addr_parse_error = None;

		let params = EngineParams {
			bind_addr: from,
			upstream: to,
		};
		match self
			.runtime
			.block_on(proxy::start_engine(params, self.build_live_config()))
		{
			Ok(handle) => self.engine = Some(handle),
			Err(err) => self.error = Some(format!("failed to start: {err}")),
		}
	}

	fn stop_engine(&mut self) {
		if let Some(engine) = self.engine.take() {
			self.runtime.block_on(engine.stop());
		}
	}
}

impl eframe::App for ProxyApp {
	fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
		let running = self.engine.is_some();

		egui::CentralPanel::default().show(ui, |ui| {
			ui.heading("UDP Conditioner Proxy");

			egui::Grid::new("addresses").num_columns(2).show(ui, |ui| {
				ui.label("Bind address");
				ui.add_enabled(!running, egui::TextEdit::singleline(&mut self.from_str));
				ui.end_row();

				ui.label("Upstream address");
				ui.add_enabled(!running, egui::TextEdit::singleline(&mut self.to_str));
				ui.end_row();
			});

			if let Some(err) = &self.addr_parse_error {
				ui.colored_label(egui::Color32::RED, err);
			}

			ui.separator();

			egui::Grid::new("params").num_columns(2).show(ui, |ui| {
				ui.label("Delay (ms)");
				ui.add(egui::DragValue::new(&mut self.delay_ms));
				ui.end_row();

				ui.label("Delay up (ms)");
				ui.add(egui::DragValue::new(&mut self.delay_up_ms));
				ui.end_row();

				ui.label("Delay down (ms)");
				ui.add(egui::DragValue::new(&mut self.delay_down_ms));
				ui.end_row();

				ui.label("Jitter (ms)");
				ui.add(egui::DragValue::new(&mut self.jitter_ms));
				ui.end_row();

				ui.label("Loss probability");
				ui.add(egui::Slider::new(&mut self.loss, 0.0..=1.0));
				ui.end_row();

				ui.label("Bandwidth limit (B/s)");
				ui.add(egui::DragValue::new(&mut self.bandwidth_limit).speed(1024.0));
				ui.end_row();

				ui.label("Bandwidth limit up (B/s)");
				ui.add(egui::DragValue::new(&mut self.bandwidth_limit_up).speed(1024.0));
				ui.end_row();

				ui.label("Bandwidth limit down (B/s)");
				ui.add(egui::DragValue::new(&mut self.bandwidth_limit_down).speed(1024.0));
				ui.end_row();
			});

			ui.separator();

			ui.horizontal(|ui| {
				if running {
					if ui.button("Stop").clicked() {
						self.stop_engine();
					}
					ui.colored_label(egui::Color32::GREEN, "Running");
				} else {
					if ui.button("Start").clicked() {
						self.start_engine();
					}
					ui.label("Stopped");
				}
			});

			if let Some(err) = &self.error {
				ui.colored_label(egui::Color32::RED, err);
			}
		});

		// push live config updates to the running engine, only on change
		if let Some(engine) = &self.engine {
			let cfg = self.build_live_config();
			if *engine.live_config_tx.borrow() != cfg {
				let _ = engine.live_config_tx.send(cfg);
			}
		}
	}

	fn on_exit(&mut self) {
		self.stop_engine();
	}
}
