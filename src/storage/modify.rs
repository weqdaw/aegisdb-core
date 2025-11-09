#[derive(Debug, Clone)]
pub enum Modify {
    Put(Put),
    Delete(Delete),
}

#[derive(Debug, Clone)]
pub struct Put {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub cf: String,
}

#[derive(Debug, Clone)]
pub struct Delete {
    pub key: Vec<u8>,
    pub cf: String,
}

impl Modify {
    pub fn key(&self) -> &[u8] {
        match self {
            Modify::Put(p) => &p.key,
            Modify::Delete(d) => &d.key,
        }
    }
    
    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Modify::Put(p) => Some(&p.value),
            Modify::Delete(_) => None,
        }
    }
    
    pub fn cf(&self) -> &str {
        match self {
            Modify::Put(p) => &p.cf,
            Modify::Delete(d) => &d.cf,
        }
    }
}