mod controller {
    use lazy_static::lazy_static;
    use rppal::pwm::{Channel, Polarity, Pwm};
    use std::env;
    use std::thread;
    use std::time::Duration;
    use tracing::{debug, error, info};

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        // Enable PWM channel 0 (BCM GPIO 18, physical pin 12) with the specified period
        let pwm = Pwm::with_frequency(
            Channel::Pwm0,
            // 25 kHz
            25_000.0,
            // 100% duty cycle
            1.0,
            // Set pin high
            Polarity::Normal,
            // Enabled
            true,
        )?;

        info!({
            hz = pwm.frequency().unwrap_or_default(),
            hot_temp = format!("{:.2}", *ICEMAN_HOT_TEMP),
            max_duty_cycle = format!("{:.2}", *ICEMAN_MAX_DUTY_CYCLE),
            min_duty_cycle = format!("{:.2}", *ICEMAN_MIN_DUTY_CYCLE),
        }, "Initialized PWM");

        thread::spawn(move || {
            // Init so the first tick resets to slow if needed.
            let mut state: Option<FanState> = None;

            loop {
                thread::sleep(Duration::from_secs(2));

                match tick(&pwm, state.clone()) {
                    Ok(new_state) => state = Some(new_state),

                    Err(err) => {
                        error!("Error from controller tick: {:?}", err);
                        continue;
                    }
                };
            }
        });

        Ok(())
    }

    #[derive(Debug, Clone)]
    enum FanState {
        Slow,
        Fast,
    }

    lazy_static! {
        pub static ref ICEMAN_HOT_TEMP: f64 = env::var("ICEMAN_HOT_TEMP")
            .unwrap_or_else(|_| "78.0".into())
            .parse::<f64>()
            .expect("variable is a valid f64");
        pub static ref ICEMAN_MAX_DUTY_CYCLE: f64 = env::var("ICEMAN_MAX_DUTY_CYCLE")
            .unwrap_or_else(|_| "1.0".into())
            .parse::<f64>()
            .expect("variable is a valid f64");
        pub static ref ICEMAN_MIN_DUTY_CYCLE: f64 = env::var("ICEMAN_MIN_DUTY_CYCLE")
            .unwrap_or_else(|_| "0.65".into())
            .parse::<f64>()
            .expect("variable is a valid f64");
    }

    fn tick(pwm: &Pwm, state: Option<FanState>) -> Result<FanState, Box<dyn std::error::Error>> {
        let temp = match crate::sensors::read_probe_temp() {
            Ok(temp) => temp as f64,
            Err(err) => {
                error!("Controller: Could not read temp sensor: {:?}", err);
                error!(
                    "Scaling fan to {:.2}% for safety.",
                    (*ICEMAN_MAX_DUTY_CYCLE * 100.0)
                );
                pwm.set_duty_cycle(*ICEMAN_MAX_DUTY_CYCLE)?;

                return Ok(FanState::Fast);
            }
        };

        debug!({
            state = format!("{:?}", state),
            temp = temp,
        }, "Current tick observation");

        let new_state = match state {
            Some(FanState::Slow) | None if temp >= *ICEMAN_HOT_TEMP => {
                info!("Increasing fan speed to max power.");
                pwm.set_duty_cycle(*ICEMAN_MAX_DUTY_CYCLE)?;

                FanState::Fast
            }
            // To avoid churning at the temp boundary we will chill things for a little longer.
            Some(FanState::Fast) | None if temp < (*ICEMAN_HOT_TEMP - 1.0) => {
                info!("Slowing fan to whisper setting.");
                pwm.set_duty_cycle(*ICEMAN_MIN_DUTY_CYCLE)?;

                FanState::Slow
            }
            // If there is no state change required, skip...
            Some(some_state) => some_state,
            None => unreachable!("State will always be set in the first two conditionals"),
        };

        Ok(new_state)
    }
}

mod sensors {
    use std::fs;
    use std::io::{self, Read};
    use std::path::Path;
    use tracing::debug;

    pub fn read_probe_temp() -> io::Result<f32> {
        let device_dir = Path::new("/sys/bus/w1/devices");
        let sensor_dir = fs::read_dir(device_dir)?
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_str().unwrap_or("").starts_with("28-"))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Temperature sensor not found")
            })?;

        debug!("reading sensor at {:?}", &sensor_dir);

        let mut content = String::new();
        fs::File::open(sensor_dir.path().join("w1_slave"))?.read_to_string(&mut content)?;

        let temp_line = content
            .lines()
            .nth(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Unexpected data format"))?;

        let temp_str = temp_line
            .rsplit_once("t=")
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Temperature value not found")
            })?
            .1;

        let temp_millicelsius: i32 = temp_str.parse().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse temperature: {}", e),
            )
        })?;

        let temp_c = temp_millicelsius as f32 / 1000.0;

        let temp_f = (temp_c * 9.0 / 5.0) + 32.0;

        Ok(temp_f)
    }

    pub fn read_cpu_temp() -> io::Result<f32> {
        let temp = std::fs::read_to_string("/sys/class/thermal/thermal_zone0/temp")?;
        let temp_c = temp.trim().parse::<f32>().unwrap_or_default() / 1000.0;
        let temp_f = (temp_c * 9.0 / 5.0) + 32.0;

        Ok(temp_f)
    }
}

mod metrics {
    use lazy_static::lazy_static;
    use rppal::gpio::{Gpio, Level as PinLevel, Trigger};
    use std::collections::HashMap;
    use std::env;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tracing::{debug, error, info};

    lazy_static! {
        pub static ref GRAFANA_API_INFLUXDB_URL: String =
            env::var("GRAFANA_API_INFLUXDB_URL").expect("GRAFANA_API_INFLUXDB_URL must be set");
        pub static ref GRAFANA_API_USERNAME: String =
            env::var("GRAFANA_API_USERNAME").expect("GRAFANA_API_USERNAME must be set");
        pub static ref GRAFANA_API_PASSWORD: String =
            env::var("GRAFANA_API_PASSWORD").expect("GRAFANA_API_PASSWORD must be set");
    }

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        // Show the env variables for grafana in debug mode.
        debug!({
            influxdb_url = GRAFANA_API_INFLUXDB_URL.as_str(),
            api_username = GRAFANA_API_USERNAME.as_str(),
            api_password = GRAFANA_API_PASSWORD.as_str(),
        }, "Grafana API credentials");

        // Set up the tach pin and rpm counter.
        let gpio = Gpio::new()?;
        let mut pin = gpio.get(17)?.into_input_pullup();
        let rpm_counter = Arc::new(RpmCounter::new());

        pin.set_async_interrupt(Trigger::Both, {
            let rpm_counter = rpm_counter.clone();
            let mut prev_level: Option<PinLevel> = None;

            // Attempt a hacky debounce since the interrupt of rppal does not currently handle
            // this.
            move |level| {
                if Some(level) == prev_level && prev_level.is_some() {
                    return;
                }
                prev_level = Some(level);

                rpm_counter.on_tick(level);
            }
        })?;

        thread::spawn(move || {
            // Keep in scope to avoid Droping the interrupt on this pin that counts rpms.
            let _pin = pin;

            loop {
                thread::sleep(Duration::from_secs(5));
                rpm_counter.compute_rpm_speed();

                let rpm_speed = rpm_counter.load_rpm_speed();
                if let Err(err) = tick(rpm_speed) {
                    error!("Error from within metrics loop: {:?}", err);
                }
            }
        });

        Ok(())
    }

    fn tick(rpm_speed: u32) -> Result<(), Box<dyn std::error::Error>> {
        publish_metric(
            "fan_controller_rpm",
            rpm_speed as f32,
            HashMap::from([("location", "kitchen"), ("fan", "fan1")]),
        )?;

        let probe_temp = crate::sensors::read_probe_temp()?;
        publish_metric(
            "fan_controller_temp",
            probe_temp,
            HashMap::from([("location", "kitchen"), ("probe", "probe1")]),
        )?;

        let cpu_temp = crate::sensors::read_cpu_temp()?;
        publish_metric(
            "fan_controller_cpu_temp",
            cpu_temp,
            HashMap::from([("location", "kitchen"), ("probe", "cpu")]),
        )?;

        info!({
            rpm_speed = format!("{:.2}", rpm_speed),
            probe_temp = format!("{:.2}", probe_temp),
            cpu_temp = format!("{:.2}", cpu_temp),
        }, "Current state from metrics");

        Ok(())
    }

    fn publish_metric(
        metric_name: &str,
        value: f32,
        attributes: HashMap<&str, &str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use reqwest::blocking::Client;

        let metric_attrs = attributes
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<String>>()
            .join(",");

        let metric = format!("{metric_name},{metric_attrs} metric={value}");

        debug!({
            metric = metric,
        }, "Publishing metric");

        let client = Client::new();
        let res = client
            .post(GRAFANA_API_INFLUXDB_URL.as_str())
            .basic_auth(
                &GRAFANA_API_USERNAME.as_str(),
                Some(GRAFANA_API_PASSWORD.as_str()),
            )
            .body(metric)
            .send()?;

        if res.status().is_success() {
            Ok(())
        } else {
            Err(format!(
                "Error: Received status code {} from metrics endpoint.",
                res.status()
            )
            .into())
        }
    }

    struct RpmCounter {
        last_read_ms: AtomicU64,
        rpm: AtomicU32,
        pulses: AtomicU64,
    }

    // Noctua fans pulse two times for each revolution.
    const FAN_PULSE: f64 = 2.0;

    impl RpmCounter {
        fn new() -> Self {
            Self {
                last_read_ms: AtomicU64::new(Self::now_as_timestamp_in_micros()),
                rpm: AtomicU32::new(0),
                pulses: AtomicU64::new(0),
            }
        }

        fn now_as_timestamp_in_micros() -> u64 {
            use std::time::{SystemTime, UNIX_EPOCH};

            let now = SystemTime::now();
            let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

            since_epoch.as_micros() as u64
        }

        fn compute_rpm_speed(&self) {
            let now = Self::now_as_timestamp_in_micros();
            let prev = self.last_read_ms.load(Ordering::Acquire);
            let dt_secs = (now - prev) as f64 / 1_000_000.0;
            let pulses = self.pulses.swap(0, Ordering::SeqCst);
            let rpm = (((pulses as f64) / dt_secs) / FAN_PULSE) * 60.0;

            debug!({
                now = now,
                prev = prev,
                dt_secs = dt_secs,
                pulses = pulses,
                rpm = rpm,
            }, "Computing the RPM periodically");

            self.rpm.store(rpm as u32, Ordering::SeqCst);
            self.last_read_ms.store(now, Ordering::SeqCst);
        }

        fn load_rpm_speed(&self) -> u32 {
            self.rpm.load(Ordering::Acquire)
        }

        fn on_tick(&self, level: PinLevel) {
            if level != PinLevel::Low {
                return;
            }
            self.pulses.fetch_add(1, Ordering::SeqCst);
        }
    }
}

use lazy_static::lazy_static;
use std::env;
use tracing::{info, Level};

lazy_static! {
    pub static ref LOG_LEVEL: String = env::var("LOG_LEVEL").unwrap_or_else(|_| "INFO".to_string());
}

fn log_level_from_env() -> Level {
    match LOG_LEVEL.as_str() {
        "DEBUG" => Level::DEBUG,
        "TRACE" => Level::TRACE,
        _ => Level::INFO,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(log_level_from_env())
        .init();

    info!("Setting log level to: {}", log_level_from_env());

    info!("Starting fan controller...");
    crate::controller::run()?;

    info!("Starting metric reporter...");
    crate::metrics::run()?;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(5))
    }
}
