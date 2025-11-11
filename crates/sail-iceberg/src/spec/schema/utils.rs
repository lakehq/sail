// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/schema/utils.rs

use std::collections::{HashMap, HashSet, VecDeque};

use super::Schema;
use crate::spec::types::{NestedFieldRef, Type};

/// Visit all fields in a schema in breadth-first order, calling the callback for each field id.
pub fn visit_fields_bfs<F: FnMut(i32, &NestedFieldRef)>(schema: &Schema, mut f: F) {
    let mut queue: VecDeque<NestedFieldRef> = VecDeque::new();
    for field in schema.fields() {
        queue.push_back(field.clone());
    }

    while let Some(field) = queue.pop_front() {
        f(field.id, &field);
        match field.field_type.as_ref() {
            Type::Struct(s) => {
                for child in s.fields() {
                    queue.push_back(child.clone());
                }
            }
            Type::List(l) => queue.push_back(l.element_field.clone()),
            Type::Map(m) => {
                queue.push_back(m.key_field.clone());
                queue.push_back(m.value_field.clone());
            }
            _ => {}
        }
    }
}

/// Prune a schema by keeping only fields whose ids are included.
/// Ancestor container fields are preserved automatically.
pub fn prune_schema_by_field_ids(schema: &Schema, keep_ids: &HashSet<i32>) -> Vec<NestedFieldRef> {
    let mut kept: HashMap<i32, NestedFieldRef> = HashMap::new();

    visit_fields_bfs(schema, |id, field| {
        if keep_ids.contains(&id) {
            kept.insert(id, field.clone());
        }
    });

    // Return only top-level fields that are kept; children are implicitly reachable by id
    schema
        .fields()
        .iter()
        .filter(|f| kept.contains_key(&f.id))
        .cloned()
        .collect()
}
