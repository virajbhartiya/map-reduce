use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use uuid::Uuid;
use num_cpus;

#[derive(Debug, Clone)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub status: TaskStatus,
    pub worker_id: Option<String>,
    pub retries: u32,
}

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: String,
    pub address: String,
    pub last_heartbeat: std::time::SystemTime,
    pub tasks: Vec<String>,
}

pub struct Coordinator {
    workers: Arc<Mutex<HashMap<String, Worker>>>,
    tasks: Arc<Mutex<HashMap<String, Task>>>,
    task_updates: broadcast::Sender<String>,
}

impl Default for Coordinator {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(100);
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            task_updates: tx,
        }
    }
}

impl Coordinator {
    pub fn new() -> Self {
        Self::new_with_threads(num_cpus::get())
    }

    pub fn new_with_threads(thread_count: usize) -> Self {
        let (tx, _) = broadcast::channel(100);
        Self {
            workers: Arc::new(Mutex::new(HashMap::with_capacity(thread_count))),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            task_updates: tx,
        }
    }

    pub fn register_worker(&self, address: String) -> String {
        let worker_id = Uuid::new_v4().to_string();
        let worker = Worker {
            id: worker_id.clone(),
            address,
            last_heartbeat: std::time::SystemTime::now(),
            tasks: Vec::new(),
        };

        self.workers.lock().unwrap().insert(worker_id.clone(), worker);
        worker_id
    }

    pub fn assign_task(&self, task_id: String, worker_id: String) -> bool {
        let mut tasks = self.tasks.lock().unwrap();
        let mut workers = self.workers.lock().unwrap();

        if let Some(task) = tasks.get_mut(&task_id) {
            if let Some(worker) = workers.get_mut(&worker_id) {
                task.status = TaskStatus::InProgress;
                task.worker_id = Some(worker_id.clone());
                worker.tasks.push(task_id);
                return true;
            }
        }
        false
    }

    pub fn update_task_status(&self, task_id: String, status: TaskStatus) {
        let mut tasks = self.tasks.lock().unwrap();
        if let Some(task) = tasks.get_mut(&task_id) {
            task.status = status;
            let _ = self.task_updates.send(task_id);
        }
    }

    pub fn heartbeat(&self, worker_id: String) -> bool {
        let mut workers = self.workers.lock().unwrap();
        if let Some(worker) = workers.get_mut(&worker_id) {
            worker.last_heartbeat = std::time::SystemTime::now();
            return true;
        }
        false
    }

    pub fn check_worker_health(&self) {
        let mut workers = self.workers.lock().unwrap();
        let mut tasks = self.tasks.lock().unwrap();
        
        let timeout = std::time::Duration::from_secs(30);
        let now = std::time::SystemTime::now();

        workers.retain(|_worker_id, worker| {
            if now.duration_since(worker.last_heartbeat).unwrap() > timeout {
                // Reassign tasks from failed worker
                for task_id in &worker.tasks {
                    if let Some(task) = tasks.get_mut(task_id) {
                        task.status = TaskStatus::Pending;
                        task.worker_id = None;
                        task.retries += 1;
                    }
                }
                false
            } else {
                true
            }
        });
    }
}
