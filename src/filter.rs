use anyhow::Context;

pub enum FilterValue {
    String(String),
    // Boolean(bool),
    Integer(i32),
    OrderedEnum(String, Vec<String>)
}

pub enum FilterOperator {
    Eq,  // ==
    Gt,  // >
    Lt,  // <
    Ge,  // >=
    Le,  // <=
}

#[derive(PartialEq)]
pub enum FilterConditional {
    /// All filters must pass before the message is consumed
    All,
    /// Any of N filters must pass before the message is consumed
    Any(usize)
}

pub struct Filter {
    pub field: String,
    pub value: FilterValue,
    pub operator: FilterOperator
}
impl Filter {
    pub fn check_match(&self, value: Vec<u8>) -> anyhow::Result<bool> {
        Ok(match &self.value {
            FilterValue::String(v) => {
                let d = String::from_utf8(value).context("parsing filter field to string")?;
                match &self.operator {
                    FilterOperator::Eq => &d == v,
                    FilterOperator::Gt => &d > v,
                    FilterOperator::Ge => &d >= v,
                    FilterOperator::Lt => &d < v,
                    FilterOperator::Le => &d <= v,
                }
            },
            // FilterValue::Boolean(b) => {
            //     let d = String::from_utf8(claim.0).context("parsing filter field to string")?;
            //     match &self.operator {
            //         FilterOperator::Equal => d == s,
            //         FilterOperator::GreaterThan => d > s,
            //         FilterOperator::LessThan => d < s,
            //     }
            // },
            FilterValue::Integer(v) => {
                let d: i32 = String::from_utf8(value).context("parsing filter field to i32")?.parse()?;
                match &self.operator {
                    FilterOperator::Eq => &d == v,
                    FilterOperator::Gt => &d > v,
                    FilterOperator::Ge => &d >= v,
                    FilterOperator::Lt => &d < v,
                    FilterOperator::Le => &d <= v,
                }
            },
            FilterValue::OrderedEnum(key, values) => {
                let tgt = String::from_utf8(value).context("parsing filter field to string")?;
                let key_index = values.iter().position(|k| k == key);
                let target_index = values.iter().position(|k| k == &tgt);

                if key_index.is_none() || target_index.is_none() { false }
                else {
                    let d = key_index.unwrap();
                    let v = target_index.unwrap();
                    match &self.operator {
                        FilterOperator::Eq => d == v,
                        FilterOperator::Gt => d > v,
                        FilterOperator::Ge => d >= v,
                        FilterOperator::Lt => d < v,
                        FilterOperator::Le => d <= v,
                    }
                }
            }
        })
    }
}

pub struct Filters {
    pub conditional: FilterConditional,
    pub filters: Vec<Filter>
}