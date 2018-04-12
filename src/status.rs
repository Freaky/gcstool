use std::time::Instant;

#[derive(Default)]
pub struct Status {
    verbosity: u32,
    stage_name: Option<String>,
    work_count: u64,
    done_count: u64,
    step: u64,
    start: Option<Instant>,
    stage_start: Option<Instant>,
}

impl Status {
    pub fn new(verbosity: u32) -> Self {
        Self {
            verbosity,
            start: Some(Instant::now()),
            ..Self::default()
        }
    }

    pub fn stage(&mut self, name: &str) {
        self.finish_stage();

        self.stage_name = Some(name.to_owned());
        self.stage_start = Some(Instant::now());
        self.done_count = 0;
    }

    pub fn set_work(&mut self, count: u64) {
        self.work_count = count;
        self.step = count / 20; // XXX clamp this
    }

    pub fn stage_work(&mut self, name: &str, work: u64) {
        self.stage(name);
        self.set_work(work);
    }

    fn print_status(&mut self) {
        let elapsed = self.start.unwrap().elapsed();
        println!(
            "{}: {} of {}, {:.1}%, {:.0}/sec",
            self.stage_name.as_ref().unwrap(),
            self.done_count,
            self.work_count,
            (self.done_count as f64 / self.work_count as f64) * 100.0,
            (self.done_count as f64) / (elapsed.as_secs() as f64)
                + (f64::from(elapsed.subsec_nanos()) / 1_000_000_000_f64)
        );
    }

    #[allow(dead_code)]
    pub fn set_work_done(&mut self, count: u64) {
        self.done_count = count;

        if self.done_count % self.step == 0 {
            self.print_status();
        }
    }

    pub fn add_work(&mut self, count: u64) {
        self.done_count += count;

        if self.done_count % self.step == 0 {
            self.print_status();
        }
    }

    pub fn incr(&mut self) {
        self.add_work(1);
    }

    pub fn finish_stage(&mut self) {
        if let Some(ref stage) = self.stage_name {
            let elapsed = self.stage_start.unwrap().elapsed();
            println!(
                "{} complete in {:.2}s",
                stage,
                (elapsed.as_secs() as f64)
                    + (f64::from(elapsed.subsec_nanos()) / 1_000_000_000_f64)
            );
        }
        self.stage_name = None;
    }

    pub fn done(mut self) {
        self.finish_stage();
        let elapsed = self.start.unwrap().elapsed();
        println!(
            "Complete in {:.2}s",
            (elapsed.as_secs() as f64) + (f64::from(elapsed.subsec_nanos()) / 1_000_000_000_f64)
        );
    }
}
