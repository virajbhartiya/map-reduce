use std::collections::HashMap;

/// Trait for implementing custom map functions
pub trait MapFunction: Send + Sync {
    /// Takes input data and returns a vector of key-value pairs
    fn map(&self, input: &str) -> Vec<(String, String)>;
}

/// Trait for implementing custom reduce functions
pub trait ReduceFunction: Send + Sync {
    /// Takes a key and iterator of values, returns a single reduced value
    fn reduce(&self, key: &str, values: Vec<String>) -> String;
}

// Built-in word count map function
pub struct WordCountMapper;

impl MapFunction for WordCountMapper {
    fn map(&self, input: &str) -> Vec<(String, String)> {
        input
            .split_whitespace()
            .map(|word| (word.to_lowercase(), "1".to_string()))
            .collect()
    }
}

// Built-in sum reduce function
pub struct SumReducer;

impl ReduceFunction for SumReducer {
    fn reduce(&self, _key: &str, values: Vec<String>) -> String {
        values
            .iter()
            .filter_map(|v| v.parse::<i64>().ok())
            .sum::<i64>()
            .to_string()
    }
}

// Registry for map and reduce functions
pub struct FunctionRegistry {
    map_functions: HashMap<String, Box<dyn MapFunction>>,
    reduce_functions: HashMap<String, Box<dyn ReduceFunction>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            map_functions: HashMap::new(),
            reduce_functions: HashMap::new(),
        };

        // Register built-in functions
        registry.register_map_function("word_count".to_string(), Box::new(WordCountMapper));
        registry.register_reduce_function("sum".to_string(), Box::new(SumReducer));

        registry
    }

    pub fn register_map_function(&mut self, name: String, function: Box<dyn MapFunction>) {
        self.map_functions.insert(name, function);
    }

    pub fn register_reduce_function(&mut self, name: String, function: Box<dyn ReduceFunction>) {
        self.reduce_functions.insert(name, function);
    }

    pub fn get_map_function(&self, name: &str) -> Option<&Box<dyn MapFunction>> {
        self.map_functions.get(name)
    }

    pub fn get_reduce_function(&self, name: &str) -> Option<&Box<dyn ReduceFunction>> {
        self.reduce_functions.get(name)
    }
}
