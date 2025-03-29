use std::pin::Pin;

use crate::mr::{KeyValue, MRApp};

pub struct WC;

impl MRApp for WC {
    fn map(&self, key: String, value: String) -> Pin<Box<(dyn Future<Output=Result<Vec<KeyValue>, anyhow::Error>> + 'static)>> {
        Box::pin(async move {
            // let re = Regex::new(r"\p{L}+").unwrap();
            // let mut res = vec![];
            // for word in re.find_iter(&value) {
            //     res.push(KeyValue {
            //         key: word.as_str().to_string(),
            //         value: "1".to_string(),
            //     });
            // }
            // regular expression is an order of magnitude slower
            let mut res = vec![];
            let mut was_letter = false;
            let mut buf = String::new();
            for c in value.chars() {
                if c.is_alphabetic() {
                    buf.push(c);
                    was_letter = true;
                } else {
                    if was_letter {
                        res.push(KeyValue { key: buf.clone(), value: "1".to_string() });
                        buf.clear();
                    }
                    was_letter = false;
                }
            }
            if buf.len() > 0 {
                res.push(KeyValue { key: buf.clone(), value: "1".to_string() });
            }
            Ok(res)
        })
    }

    fn reduce(&self, key: String, value: Vec<String>) -> Pin<Box<dyn Future<Output=Result<String, anyhow::Error>> + 'static>> {
        Box::pin(async move {
            Ok(value.len().to_string())
        })
    }
}
