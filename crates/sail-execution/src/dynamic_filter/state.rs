use std::collections::{BTreeMap, HashMap};

use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;

use super::DynamicFilterRoute;
use super::wire::{MAX_FILTER_BYTES, true_predicate, union_predicates};
use crate::driver::r#gen::{DynamicFilterUpdate, ExchangeDynamicFiltersResponse};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskKey;

pub(crate) struct DynamicFilterState {
    revision: u64,
    updates: HashMap<(u64, TaskKey), DynamicFilterUpdate>,
    byte_limit: usize,
    disabled: bool,
}

impl Default for DynamicFilterState {
    fn default() -> Self {
        Self {
            revision: 0,
            updates: HashMap::new(),
            byte_limit: 16 * 1024 * 1024,
            disabled: false,
        }
    }
}

impl DynamicFilterState {
    pub fn exchange(
        &mut self,
        key: &TaskKey,
        routes: &BTreeMap<u64, DynamicFilterRoute>,
        attempts: &HashMap<(usize, usize), usize>,
        updates: Vec<DynamicFilterUpdate>,
        revision: u64,
    ) -> ExecutionResult<ExchangeDynamicFiltersResponse> {
        if attempts.get(&(key.stage, key.partition)) != Some(&key.attempt) {
            return Err(ExecutionError::InvalidArgument(
                "stale dynamic filter task attempt".into(),
            ));
        }
        let count = self.updates.len();
        self.updates.retain(|(_, task), _| {
            attempts.get(&(task.stage, task.partition)) == Some(&task.attempt)
        });
        if self.updates.len() != count {
            self.revision += 1;
        }
        let mut bytes = self.updates.values().map(update_size).sum::<usize>();
        for update in updates {
            let route = routes.get(&update.expression_id).ok_or_else(|| {
                ExecutionError::InvalidArgument("unknown dynamic filter ID".into())
            })?;
            if !route
                .producers
                .get(&key.stage)
                .is_some_and(|count| key.partition < *count)
            {
                return Err(ExecutionError::InvalidArgument(
                    "task does not produce this dynamic filter".into(),
                ));
            }
            if update.predicate.len() > MAX_FILTER_BYTES || update.schema.len() > MAX_FILTER_BYTES {
                return Err(ExecutionError::InvalidArgument(
                    "dynamic filter update exceeds size limit".into(),
                ));
            }
            PhysicalExprNode::decode(update.predicate.as_slice()).map_err(|e| {
                ExecutionError::InvalidArgument(format!("invalid dynamic predicate: {e}"))
            })?;
            crate::proto::decode::try_decode_schema(&update.schema)?;
            if self.disabled {
                continue;
            }
            let slot = (update.expression_id, key.clone());
            if let Some(previous) = self.updates.get(&slot)
                && (update.generation < previous.generation
                    || (update.generation == previous.generation
                        && (!update.complete || previous.complete)))
            {
                continue;
            }
            bytes = bytes - self.updates.get(&slot).map(update_size).unwrap_or(0)
                + update_size(&update);
            if bytes > self.byte_limit {
                // Dynamic filters are optional. Release all retained snapshots
                // and let this job continue without remote pruning.
                self.updates.clear();
                self.disabled = true;
                self.revision += 1;
                break;
            }
            self.updates.insert(slot, update);
            self.revision += 1;
        }
        let mut response = ExchangeDynamicFiltersResponse {
            revision: self.revision,
            updates: vec![],
        };
        if revision == self.revision {
            return Ok(response);
        }
        for (id, route) in routes {
            if !route.consumers.contains(&key.stage) {
                continue;
            }
            if self.disabled {
                response.updates.push(DynamicFilterUpdate {
                    expression_id: *id,
                    generation: self.revision,
                    predicate: true_predicate()?,
                    schema: vec![],
                    complete: true,
                });
                continue;
            }
            let updates = route
                .producers
                .iter()
                .flat_map(|(stage, count)| {
                    (0..*count).filter_map(|partition| {
                        let attempt = attempts.get(&(*stage, partition))?;
                        self.updates.get(&(
                            *id,
                            TaskKey {
                                job_id: key.job_id,
                                stage: *stage,
                                partition,
                                attempt: *attempt,
                            },
                        ))
                    })
                })
                .collect::<Vec<_>>();
            let Some(first) = updates.first() else {
                continue;
            };
            if updates.iter().any(|update| update.schema != first.schema) {
                return Err(ExecutionError::InvalidArgument(
                    "dynamic filter input schemas disagree".into(),
                ));
            }
            let all_present = updates.len() == route.producers.values().sum::<usize>();
            let predicate = if !all_present {
                true_predicate()?
            } else if updates.len() == 1 {
                first.predicate.clone()
            } else {
                union_predicates(&updates)?
            };
            response.updates.push(DynamicFilterUpdate {
                expression_id: *id,
                generation: self.revision,
                predicate,
                schema: first.schema.clone(),
                complete: all_present && updates.iter().all(|update| update.complete),
            });
        }
        Ok(response)
    }

    pub fn clear(&mut self) {
        self.updates.clear();
    }
}

fn update_size(update: &DynamicFilterUpdate) -> usize {
    update.predicate.len()
        + update.schema.len()
        + std::mem::size_of::<(TaskKey, DynamicFilterUpdate)>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Result;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::dynamic_filter::wire::{DynamicFilterBinding, snapshot};

    fn task(stage: usize, partition: usize, attempt: usize) -> TaskKey {
        TaskKey {
            job_id: 1.into(),
            stage,
            partition,
            attempt,
        }
    }

    fn filter() -> Arc<dyn PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("k", 0))],
            lit(true),
        ))
    }

    fn update(
        expr: &Arc<dyn PhysicalExpr>,
        value: i64,
        complete: bool,
    ) -> Result<DynamicFilterUpdate> {
        let dynamic = expr
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .ok_or_else(|| datafusion::common::internal_datafusion_err!("missing filter"))?;
        dynamic.update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("k", 0)),
            Operator::Eq,
            lit(value),
        )))?;
        snapshot(
            expr,
            &Schema::new(vec![Field::new("k", DataType::Int64, true)]),
            complete,
        )
    }

    fn evaluate(binding: &DynamicFilterBinding) -> Result<Vec<Option<bool>>> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
            ]))],
        )?;
        let values = binding.filter.evaluate(&batch)?.into_array(4)?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::common::internal_datafusion_err!("expected Boolean array")
            })?;
        Ok(values.iter().collect())
    }

    #[test]
    fn waits_for_all_partitions_and_unions_their_predicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let expr = filter();
        let first = update(&expr, 1, true)?;
        let id = first.expression_id;
        let mut second = update(&expr, 3, true)?;
        second.generation = first.generation;
        let routes = BTreeMap::from([(
            id,
            DynamicFilterRoute {
                producers: BTreeMap::from([(1, 2)]),
                consumers: [0].into(),
            },
        )]);
        let attempts = HashMap::from([((1, 0), 0), ((1, 1), 0), ((0, 0), 0)]);
        let mut state = DynamicFilterState::default();
        state.exchange(&task(1, 0, 0), &routes, &attempts, vec![first.clone()], 0)?;
        let response = state.exchange(&task(0, 0, 0), &routes, &attempts, vec![], 0)?;
        assert_eq!(response.updates.len(), 1);
        assert!(!response.updates[0].complete);
        let binding = DynamicFilterBinding { filter: filter() };
        binding.apply(&response.updates[0], &SessionContext::new().task_ctx())?;
        assert_eq!(evaluate(&binding)?, vec![Some(true); 4]);
        state.exchange(&task(1, 1, 0), &routes, &attempts, vec![second], 0)?;
        let response = state.exchange(
            &task(0, 0, 0),
            &routes,
            &attempts,
            vec![],
            response.revision,
        )?;
        assert!(response.updates[0].complete);
        binding.apply(&response.updates[0], &SessionContext::new().task_ctx())?;
        assert_eq!(
            evaluate(&binding)?,
            vec![Some(true), Some(false), Some(true), None]
        );
        // An idempotent RPC retry cannot advance the revision or overwrite a final value.
        let duplicate = state.exchange(
            &task(1, 0, 0),
            &routes,
            &attempts,
            vec![first],
            response.revision,
        )?;
        assert_eq!(duplicate.revision, response.revision);
        assert!(duplicate.updates.is_empty());
        Ok(())
    }

    #[test]
    fn retry_rejects_stale_attempts_and_discards_old_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let expr = filter();
        let first = update(&expr, 1, true)?;
        let id = first.expression_id;
        let routes = BTreeMap::from([(
            id,
            DynamicFilterRoute {
                producers: BTreeMap::from([(1, 1)]),
                consumers: [0].into(),
            },
        )]);
        let mut attempts = HashMap::from([((1, 0), 0), ((0, 0), 0)]);
        let mut state = DynamicFilterState::default();
        state.exchange(&task(1, 0, 0), &routes, &attempts, vec![first.clone()], 0)?;
        attempts.insert((1, 0), 1);
        attempts.insert((0, 0), 1);
        assert!(
            state
                .exchange(&task(1, 0, 0), &routes, &attempts, vec![first.clone()], 0)
                .is_err()
        );
        let response = state.exchange(&task(0, 0, 1), &routes, &attempts, vec![], 0)?;
        assert!(response.updates.is_empty());
        assert!(state.updates.is_empty());
        let mut replacement = update(&expr, 2, true)?;
        replacement.generation = first.generation;
        state.exchange(&task(1, 0, 1), &routes, &attempts, vec![replacement], 0)?;
        let response = state.exchange(
            &task(0, 0, 1),
            &routes,
            &attempts,
            vec![],
            response.revision,
        )?;
        let binding = DynamicFilterBinding { filter: filter() };
        binding.apply(&response.updates[0], &SessionContext::new().task_ctx())?;
        assert_eq!(
            evaluate(&binding)?,
            vec![Some(false), Some(true), Some(false), None]
        );
        state.clear();
        assert!(state.updates.is_empty());
        Ok(())
    }

    #[test]
    fn rejects_unknown_producers_and_invalid_payloads() -> Result<(), Box<dyn std::error::Error>> {
        let first = update(&filter(), 1, true)?;
        let routes = BTreeMap::from([(
            first.expression_id,
            DynamicFilterRoute {
                producers: BTreeMap::from([(1, 1)]),
                consumers: [0].into(),
            },
        )]);
        let attempts = HashMap::from([((1, 0), 0), ((0, 0), 0)]);
        let mut state = DynamicFilterState::default();
        assert!(
            state
                .exchange(&task(0, 0, 0), &routes, &attempts, vec![first.clone()], 0)
                .is_err()
        );
        let mut malformed = first.clone();
        malformed.predicate = vec![255];
        assert!(
            state
                .exchange(&task(1, 0, 0), &routes, &attempts, vec![malformed], 0)
                .is_err()
        );
        let mut oversized = first.clone();
        oversized.predicate = vec![0; MAX_FILTER_BYTES + 1];
        assert!(
            state
                .exchange(&task(1, 0, 0), &routes, &attempts, vec![oversized], 0)
                .is_err()
        );
        let mut unknown = first;
        unknown.expression_id += 1;
        assert!(
            state
                .exchange(&task(1, 0, 0), &routes, &attempts, vec![unknown], 0)
                .is_err()
        );
        assert!(state.updates.is_empty());
        Ok(())
    }

    #[test]
    fn exhausted_driver_budget_releases_snapshots_and_fails_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let update = update(&filter(), 1, true)?;
        let id = update.expression_id;
        let routes = BTreeMap::from([(
            id,
            DynamicFilterRoute {
                producers: BTreeMap::from([(1, 1)]),
                consumers: [0].into(),
            },
        )]);
        let attempts = HashMap::from([((1, 0), 0), ((0, 0), 0)]);
        let mut state = DynamicFilterState {
            byte_limit: 0,
            ..Default::default()
        };
        state.exchange(&task(1, 0, 0), &routes, &attempts, vec![update], 0)?;
        let response = state.exchange(&task(0, 0, 0), &routes, &attempts, vec![], 0)?;
        assert!(state.updates.is_empty());
        assert!(response.updates[0].complete);
        let binding = DynamicFilterBinding { filter: filter() };
        binding.apply(&response.updates[0], &SessionContext::new().task_ctx())?;
        assert_eq!(evaluate(&binding)?, vec![Some(true); 4]);
        Ok(())
    }
}
