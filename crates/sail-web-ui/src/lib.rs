use std::net::SocketAddr;

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use sail_common_datafusion::system::observable::{SessionManagerObserver, StateObservable};
use sail_common_datafusion::system::predicate::Predicates;
use sail_session::session_manager::SessionManager;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(Clone)]
struct UiState {
    session_manager: SessionManager,
}

#[derive(Serialize)]
struct ApiError {
    error: String,
}

struct ApiResult<T>(DataFusionResult<T>);

impl<T> IntoResponse for ApiResult<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        match self.0 {
            Ok(value) => Json(value).into_response(),
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError {
                    error: error.to_string(),
                }),
            )
                .into_response(),
        }
    }
}

pub async fn serve(
    listener: TcpListener,
    session_manager: SessionManager,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = listener.local_addr()?;
    log::info!("Starting the Sail Web UI on http://{address}");
    let app = router(session_manager);
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn serve_on(
    address: SocketAddr,
    session_manager: SessionManager,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(address).await?;
    serve(listener, session_manager).await
}

fn router(session_manager: SessionManager) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/history/{app_id}", get(index))
        .route("/history/{app_id}/", get(index))
        .route("/history/{app_id}/jobs/", get(index))
        .route("/history/{app_id}/stages/", get(index))
        .route("/history/{app_id}/tasks/", get(index))
        .route("/history/{app_id}/executors/", get(index))
        .route("/api/sessions", get(sessions))
        .route("/api/jobs", get(jobs))
        .route("/api/jobs/{job_id}", get(job))
        .route("/api/jobs/{job_id}/stages", get(job_stages))
        .route("/api/jobs/{job_id}/tasks", get(job_tasks))
        .route("/api/stages", get(stages))
        .route("/api/tasks", get(tasks))
        .route("/api/workers", get(workers))
        .fallback(get(index))
        .with_state(UiState { session_manager })
}

async fn observe<T>(
    session_manager: &SessionManager,
    make_observer: impl FnOnce(oneshot::Sender<DataFusionResult<Vec<T>>>) -> SessionManagerObserver,
) -> DataFusionResult<Vec<T>>
where
    T: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    session_manager.observe(make_observer(tx)).await;
    rx.await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
}

async fn sessions(State(state): State<UiState>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Sessions {
                session_id: Predicates::always_true(),
                fetch: usize::MAX,
                result,
            }
        })
        .await,
    )
}

async fn jobs(State(state): State<UiState>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Jobs {
                session_id: Predicates::always_true(),
                job_id: Predicates::always_true(),
                fetch: usize::MAX,
                result,
            }
        })
        .await,
    )
}

async fn job(State(state): State<UiState>, Path(job_id): Path<u64>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Jobs {
                session_id: Predicates::always_true(),
                job_id: equal_u64(job_id),
                fetch: 1,
                result,
            }
        })
        .await,
    )
}

async fn stages(State(state): State<UiState>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Stages {
                session_id: Predicates::always_true(),
                job_id: Predicates::always_true(),
                fetch: usize::MAX,
                result,
            }
        })
        .await,
    )
}

async fn job_stages(State(state): State<UiState>, Path(job_id): Path<u64>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Stages {
                session_id: Predicates::always_true(),
                job_id: equal_u64(job_id),
                fetch: usize::MAX,
                result,
            }
        })
        .await,
    )
}

async fn tasks(State(state): State<UiState>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Tasks {
                session_id: Predicates::always_true(),
                job_id: Predicates::always_true(),
                fetch: usize::MAX,
                result,
            }
        })
        .await,
    )
}

async fn job_tasks(State(state): State<UiState>, Path(job_id): Path<u64>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Tasks {
                session_id: Predicates::always_true(),
                job_id: equal_u64(job_id),
                fetch: usize::MAX,
                result,
            }
        })
        .await,
    )
}

async fn workers(State(state): State<UiState>) -> impl IntoResponse {
    ApiResult(
        observe(&state.session_manager, |result| {
            SessionManagerObserver::Workers {
                session_id: Predicates::always_true(),
                worker_id: Predicates::always_true(),
                fetch: usize::MAX,
                result,
            }
        })
        .await,
    )
}

fn equal_u64(expected: u64) -> sail_common_datafusion::system::predicate::Predicate<u64> {
    std::sync::Arc::new(move |value| Ok(*value == expected))
}

async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(INDEX_HTML),
    )
}

const INDEX_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Sail Web UI</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #ffffff;
      --panel: #ffffff;
      --text: #333333;
      --muted: #777777;
      --line: #dddddd;
      --accent: #337ab7;
      --accent-dark: #23527c;
      --tab: #f5f5f5;
      --ok: #3c763d;
      --bad: #a94442;
      --warn: #8a6d3b;
      --spark-orange: #f15a24;
      --spark-dag-bg: #A0DFFF;
      --spark-dag-node: #eaf6fb;
      --spark-dag-line: #31708f;
      --spark-dag-text: #31708f;
      --navbar: #222222;
      --navbar-border: #080808;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.42857143 "Helvetica Neue", Helvetica, Arial, sans-serif;
    }
    header {
      min-height: 50px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 15px;
      border-bottom: 1px solid var(--navbar-border);
      background: var(--navbar);
      color: #fff;
      position: static;
    }
    .navbar-brand {
      display: inline-flex;
      align-items: center;
      min-height: 50px;
      padding: 0 15px 0 0;
      color: #ffffff;
      font-size: 18px;
      font-weight: 500;
      letter-spacing: 0;
    }
    main { width: 100%; max-width: 1600px; margin: 0 auto; padding: 0 15px 36px; }
    nav { display: flex; gap: 0; align-items: center; }
    button {
      border: 1px solid transparent;
      background: transparent;
      color: #9d9d9d;
      min-height: 50px;
      padding: 0 15px;
      border-radius: 0;
      cursor: pointer;
      font: inherit;
    }
    button:hover { color: #fff; }
    button.active { background: #080808; border-color: #080808; color: #fff; font-weight: 500; }
    #refresh {
      margin-left: 10px;
      min-height: 34px;
      padding: 0 12px;
      border-radius: 4px;
      border-color: #555555;
      color: #eeeeee;
    }
    #refresh:hover { background: #333333; }
    .app-header {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-end;
      padding: 16px 0 10px;
      border-bottom: 1px solid #eeeeee;
      margin-bottom: 14px;
    }
    .app-header h1 {
      margin: 0;
      font-size: 24px;
      font-weight: 500;
      line-height: 1.1;
    }
    .app-meta {
      color: var(--muted);
      font-size: 12px;
      text-align: right;
      white-space: nowrap;
    }
    .page-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin: 0 0 12px;
    }
    .page-title h2 {
      margin: 0;
      font-size: 21px;
      font-weight: 500;
    }
    .subnav {
      display: flex;
      gap: 0;
      margin: 0 0 15px;
      border-bottom: 1px solid #dddddd;
    }
    .subnav a {
      display: inline-flex;
      align-items: center;
      min-height: 36px;
      padding: 0 14px;
      border: 1px solid transparent;
      border-bottom: 0;
      background: transparent;
      color: var(--muted);
      font-weight: 500;
    }
    .subnav a.active {
      background: #fff;
      color: #555555;
      border-color: #dddddd;
      border-radius: 4px 4px 0 0;
      margin-bottom: -1px;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 0;
      margin-bottom: 15px;
      border: 1px solid var(--line);
      border-radius: 4px;
      overflow: hidden;
    }
    .metric, section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 4px;
    }
    .metric {
      padding: 10px 12px;
      border: 0;
      border-right: 1px solid var(--line);
      border-radius: 0;
    }
    .metric:last-child { border-right: 0; }
    .metric strong { display: block; font-size: 22px; font-weight: 500; line-height: 1.1; }
    .metric span { color: var(--muted); font-size: 12px; text-transform: none; }
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      padding: 12px;
    }
    .metric.small strong { font-size: 18px; }
    .metric.small span { text-transform: none; }
    .metric-table td:first-child { color: var(--muted); white-space: nowrap; }
    .kv {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 0;
    }
    .kv div {
      padding: 9px 10px;
      border-right: 1px solid var(--line);
      border-bottom: 1px solid var(--line);
    }
    .kv div:nth-child(4n) { border-right: 0; }
    .kv span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 2px;
    }
    .timeline {
      padding: 12px;
      overflow: auto;
    }
    .timeline-row {
      display: grid;
      grid-template-columns: 130px minmax(420px, 1fr) 90px;
      gap: 10px;
      align-items: center;
      min-height: 30px;
    }
    .timeline-track {
      position: relative;
      height: 18px;
      background: #f5f5f5;
      border: 1px solid var(--line);
      border-radius: 3px;
    }
    .timeline-bar {
      position: absolute;
      top: 2px;
      min-width: 2px;
      height: 12px;
      border-radius: 2px;
      background: #dff0d8;
      border: 1px solid #3c763d;
    }
    .timeline-bar.running { background: #A0DFFF; border-color: #337ab7; }
    .timeline-bar.failed { background: #f2dede; border-color: #a94442; }
    section { margin-bottom: 15px; overflow: hidden; }
    section h2 {
      margin: 0;
      padding: 10px 12px;
      font-size: 15px;
      font-weight: 500;
      border-bottom: 1px solid var(--line);
      background: #f5f5f5;
    }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 8px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th { color: #333333; font-weight: 700; font-size: 12px; background: #f5f5f5; }
    tbody tr:nth-child(odd) { background: #f9f9f9; }
    tr:last-child td { border-bottom: 0; }
    a { color: var(--accent); text-decoration: none; font-weight: 650; }
    .status { font-weight: 700; }
    .status.SUCCEEDED, .status.RUNNING, .status.ACTIVE { color: var(--ok); }
    .status.FAILED { color: var(--bad); }
    .status.CANCELED { color: var(--warn); }
    .muted { color: var(--muted); }
    .dag-wrap {
      overflow: auto;
      padding: 12px;
      background: var(--spark-dag-bg);
    }
    .dag-svg {
      display: block;
      min-width: 100%;
      background: var(--spark-dag-bg);
      border: 1px solid #7EC8EA;
      border-radius: 4px;
    }
    .dag-edge { stroke: var(--spark-dag-line); stroke-width: 1.6; fill: none; marker-end: url(#arrow); }
    .dag-node { fill: var(--spark-dag-node); stroke: #66afe9; stroke-width: 1.3; }
    .dag-node.running { fill: #A0DFFF; stroke: #337ab7; stroke-width: 2; }
    .dag-node.succeeded { fill: #dff0d8; stroke: #3c763d; }
    .dag-node.failed { fill: #f2dede; stroke: var(--bad); stroke-width: 2; }
    .dag-title { font: 650 13px ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: var(--spark-dag-text); }
    .dag-meta { font: 12px ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #31708f; }
    .dag-badge { fill: var(--spark-orange); }
    .operator-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
      padding: 12px;
    }
    .operator-panel {
      border: 1px solid var(--line);
      border-radius: 4px;
      overflow: hidden;
      background: #fff;
    }
    .operator-panel h3 {
      margin: 0;
      padding: 8px 10px;
      font-size: 13px;
      background: #f5f5f5;
      border-bottom: 1px solid var(--line);
    }
    .operator-svg {
      display: block;
      min-width: 100%;
      background: var(--spark-dag-bg);
    }
    .operator-scroll {
      overflow: auto;
      background: var(--spark-dag-bg);
    }
    .operator-edge { stroke: var(--spark-dag-line); stroke-width: 1.4; fill: none; marker-end: url(#arrow); }
    .operator-node { fill: var(--spark-dag-node); stroke: #66afe9; stroke-width: 1.2; }
    .operator-node.exchange { fill: #fcf8e3; stroke: var(--spark-orange); stroke-width: 2; }
    .operator-node.join { fill: #A0DFFF; stroke: #337ab7; stroke-width: 2; }
    .operator-node.filter { fill: #dff0d8; stroke: #3c763d; stroke-width: 2; }
    .operator-node.project { fill: #eaf6fb; stroke: #31708f; stroke-width: 2; }
    .operator-title { font: 650 12px ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: var(--spark-dag-text); }
    .operator-meta { font: 11px ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #31708f; }
    .operator-metric { font: 650 11px ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #333333; }
    details.plan-text {
      border: 1px solid var(--line);
      border-radius: 4px;
      background: #fff;
      margin: 12px;
    }
    details.plan-text summary {
      cursor: pointer;
      padding: 8px 10px;
      background: #f5f5f5;
      border-bottom: 1px solid var(--line);
      font-weight: 650;
    }
    details.metric-details {
      border-top: 1px solid var(--line);
    }
    details.metric-details summary {
      cursor: pointer;
      padding: 8px 10px;
      background: #fff;
      font-weight: 650;
    }
    details.job-preview {
      border-top: 1px solid var(--line);
      background: #fff;
    }
    details.job-preview summary {
      cursor: pointer;
      padding: 9px 10px;
      background: #f5f5f5;
      border-bottom: 1px solid var(--line);
      font-weight: 650;
    }
    .job-preview-body {
      padding: 12px;
    }
    .job-preview-body section {
      margin-bottom: 12px;
    }
    pre {
      margin: 0;
      padding: 10px;
      overflow: auto;
      font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      color: #333333;
      background: #ffffff;
    }
    .back-row { margin: 0 0 12px; }
    .error { padding: 16px; color: var(--bad); }
    @media (max-width: 760px) {
      header { height: auto; min-height: 52px; align-items: flex-start; flex-direction: column; gap: 0; padding: 0 14px 10px; }
      nav { flex-wrap: wrap; }
      main { padding: 0 12px 24px; }
      .app-header { align-items: flex-start; flex-direction: column; gap: 6px; }
      .app-meta { text-align: left; white-space: normal; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .metric:nth-child(2n) { border-right: 0; }
      .metric:nth-child(-n + 2) { border-bottom: 1px solid var(--line); }
      .metrics-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kv { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kv div:nth-child(4n) { border-right: 1px solid var(--line); }
      .kv div:nth-child(2n) { border-right: 0; }
      .timeline-row { grid-template-columns: 90px minmax(260px, 1fr) 70px; }
      table { font-size: 12px; }
      th, td { padding: 7px 8px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="navbar-brand">Sail Web UI</div>
    <nav>
      <button data-view="jobs" class="active">Jobs</button>
      <button data-view="stages">Stages</button>
      <button data-view="tasks">Tasks</button>
      <button data-view="workers">Executors</button>
      <button id="refresh">Refresh</button>
    </nav>
  </header>
  <main id="app"></main>
  <script>
    const initialRoute = routeFromLocation();
    const state = { view: initialRoute.view, selectedJob: initialRoute.jobId, appId: initialRoute.appId, data: null };
    const app = document.getElementById('app');
    const buttons = [...document.querySelectorAll('button[data-view]')];
    activateNav();
    buttons.forEach(button => button.addEventListener('click', () => {
      state.view = button.dataset.view;
      state.selectedJob = null;
      activateNav();
      pushRoute();
      load();
    }));
    document.getElementById('refresh').addEventListener('click', load);
    window.addEventListener('popstate', () => {
      const route = routeFromLocation();
      state.view = route.view;
      state.selectedJob = route.jobId;
      state.appId = route.appId;
      activateNav();
      if (state.data) render();
      else load();
    });

    async function api(path) {
      const response = await fetch(path);
      if (!response.ok) throw new Error((await response.json()).error || response.statusText);
      return response.json();
    }

    async function load() {
      app.innerHTML = '<p class="muted">Loading...</p>';
      try {
        const [sessions, jobs, stages, tasks, workers] = await Promise.all([
          api('/api/sessions'), api('/api/jobs'), api('/api/stages'), api('/api/tasks'), api('/api/workers')
        ]);
        state.data = { sessions, jobs, stages, tasks, workers };
        render();
      } catch (error) {
        app.innerHTML = `<section><div class="error">${escapeHtml(error.message)}</div></section>`;
      }
    }

    function render() {
      const { sessions, jobs, stages, tasks, workers } = state.data;
      const runningTasks = tasks.filter(x => x.status === 'RUNNING').length;
      app.innerHTML = `
        ${appHeader(sessions)}
        ${pageTitle(titleForView(state.view), 'Use Refresh to update')}
        ${subnav(state.view)}
        <div class="summary">
          ${metric(jobs.length, 'Jobs')}
          ${metric(stages.length, 'Stages')}
          ${metric(tasks.length, 'Tasks')}
          ${metric(runningTasks, 'Running Tasks')}
        </div>
        ${state.view === 'jobs' ? jobsView(jobs, stages, tasks) : ''}
        ${state.view === 'stages' ? stagesView(stages, tasks) : ''}
        ${state.view === 'tasks' ? tableSection('Tasks', tasksTable(tasks)) : ''}
        ${state.view === 'workers' ? tableSection('Executors', workersTable(workers)) + tableSection('Sessions', sessionsTable(sessions)) : ''}
      `;
      [...app.querySelectorAll('[data-job]')].forEach(link => link.addEventListener('click', event => {
        event.preventDefault();
        state.selectedJob = Number(link.dataset.job);
        pushRoute();
        renderJobDetail(state.selectedJob);
      }));
      [...app.querySelectorAll('[data-view-link]')].forEach(link => link.addEventListener('click', event => {
        event.preventDefault();
        state.view = link.dataset.viewLink;
        state.selectedJob = null;
        activateNav();
        pushRoute();
        render();
      }));
      if (state.selectedJob != null) renderJobDetail(state.selectedJob);
    }

    function renderJobDetail(jobId) {
      const { sessions, jobs, stages, tasks } = state.data;
      const job = jobs.find(x => x.job_id === jobId);
      const session = job ? sessions.find(x => x.session_id === job.session_id) : null;
      const jobStages = stages.filter(x => x.job_id === jobId);
      const jobTasks = tasks.filter(x => x.job_id === jobId);
      app.innerHTML = `
        ${appHeader(sessions)}
        ${pageTitle(`Job ${jobId}`, job ? `${job.status} · ${duration(job.created_at, job.stopped_at)}` : '')}
        <p class="back-row"><a href="#" id="back">Back to Jobs</a></p>
        ${tableSection(`Job ${jobId}`, jobsTable(job ? [job] : []))}
        <section><h2>Job Details</h2>${jobDetails(job, session, jobStages, jobTasks)}</section>
        <section><h2>Event Timeline</h2>${eventTimeline(jobStages)}</section>
        <section><h2>Summary Metrics</h2>${summaryMetrics(jobStages, jobTasks)}</section>
        <section><h2>DAG Visualization</h2><div class="dag-wrap">${stageDag(jobStages)}</div></section>
        <section><h2>Physical Operator DAG</h2>${physicalOperatorDag(jobStages)}</section>
        <section><h2>Operator Metrics</h2>${operatorMetricsSections(jobStages)}</section>
        <section><h2>Physical Plans</h2>${physicalPlanText(jobStages)}</section>
        ${tableSection('Active Stages', stagesTable(jobStages.filter(x => !x.stopped_at), jobTasks))}
        ${tableSection('Completed Stages', stagesTable(jobStages.filter(x => x.stopped_at), jobTasks))}
        <section><h2>Task Metrics Summary</h2>${taskMetricsSummary(jobTasks)}</section>
        ${tableSection('Tasks', tasksTable(jobTasks))}
      `;
      document.getElementById('back').addEventListener('click', event => {
        event.preventDefault();
        state.view = 'jobs';
        state.selectedJob = null;
        activateNav();
        pushRoute();
        render();
      });
    }

    function jobsView(jobs, stages, tasks) {
      const activeJobs = jobs.filter(job => !job.stopped_at);
      const completedJobs = jobs.filter(job => job.stopped_at);
      return tableSection('Active Jobs', jobsTable(activeJobs, stages, tasks))
        + tableSection('Completed Jobs', jobsTable(completedJobs, stages, tasks))
        + tableSection('Job DAGs', jobPreviewPanels(jobs, stages, tasks));
    }

    function stagesView(stages, tasks) {
      const failedStages = stages.filter(stage => stage.status === 'FAILED');
      const activeStages = stages.filter(stage => !stage.stopped_at && stage.status !== 'FAILED');
      const completedStages = stages.filter(stage => stage.stopped_at && stage.status !== 'FAILED');
      return tableSection('Active Stages', stagesTable(activeStages, tasks))
        + tableSection('Completed Stages', stagesTable(completedStages, tasks))
        + (failedStages.length ? tableSection('Failed Stages', stagesTable(failedStages, tasks)) : '');
    }

    function jobsTable(jobs, stages = [], tasks = []) {
      return table(['Job', 'Session', 'Status', 'Stages', 'Tasks', 'Started', 'Duration', 'Details'], jobs.map(job => [
        `<a href="#" data-job="${job.job_id}">${job.job_id}</a>`,
        escapeHtml(job.session_id),
        status(job.status),
        stages.filter(x => x.job_id === job.job_id).length,
        tasks.filter(x => x.job_id === job.job_id).length,
        time(job.created_at),
        duration(job.created_at, job.stopped_at),
        `<a href="#" data-job="${job.job_id}">Details for Job ${job.job_id}</a>`,
      ]));
    }

    function jobPreviewPanels(jobs, stages, tasks) {
      if (!jobs.length) return '<div class="error muted">No jobs.</div>';
      return [...jobs].sort((a, b) => b.job_id - a.job_id).map((job, index) => {
        const jobStages = stages.filter(x => x.job_id === job.job_id);
        const jobTasks = tasks.filter(x => x.job_id === job.job_id);
        return `
          <details class="job-preview" ${index === 0 ? 'open' : ''}>
            <summary>Job ${job.job_id} · ${escapeHtml(job.status)} · ${jobStages.length} stages · ${jobTasks.length} tasks</summary>
            <div class="job-preview-body">
              <p class="back-row"><a href="#" data-job="${job.job_id}">Details for Job ${job.job_id}</a></p>
              <section><h2>Summary Metrics</h2>${summaryMetrics(jobStages, jobTasks)}</section>
              <section><h2>DAG Visualization</h2><div class="dag-wrap">${stageDag(jobStages)}</div></section>
              <section><h2>Physical Operator DAG</h2>${physicalOperatorDag(jobStages)}</section>
              <section><h2>Physical Plans</h2>${physicalPlanText(jobStages)}</section>
            </div>
          </details>
        `;
      }).join('');
    }

    function stagesTable(stages, tasks = []) {
      return table(['Job', 'Session', 'Stage', 'Status', 'Submitted', 'Duration', 'Tasks', 'Input', 'Output', 'Shuffle Read', 'Shuffle Write', 'Spill'], stages.map(stage => {
        const summary = stageSummary(stage);
        const stageTasks = tasks.filter(task => task.job_id === stage.job_id && task.stage === stage.stage);
        const succeeded = stageTasks.filter(task => task.status === 'SUCCEEDED').length;
        return [
        `<a href="#" data-job="${stage.job_id}">${stage.job_id}</a>`,
        escapeHtml(stage.session_id),
        stage.stage,
        status(stage.status),
        time(stage.created_at),
        duration(stage.created_at, stage.stopped_at),
        `${succeeded}/${stageTasks.length || stage.partitions}`,
        formatCount(summary.input_rows),
        formatCount(summary.output_rows),
        formatBytes(summary.shuffle_read_bytes),
        formatBytes(summary.shuffle_write_bytes),
        formatBytes(summary.spilled_bytes),
      ]}));
    }

    function tasksTable(tasks) {
      return table(['Job', 'Session', 'Stage', 'Partition', 'Attempt', 'Status', 'Launch Time', 'Duration', 'Input', 'Output', 'Shuffle Read', 'Shuffle Write', 'Spill', 'Peak Memory', 'Compute Time'], tasks.map(task => {
        const summary = taskSummary(task);
        return [
        `<a href="#" data-job="${task.job_id}">${task.job_id}</a>`,
        escapeHtml(task.session_id),
        task.stage,
        task.partition,
        task.attempt,
        status(task.status),
        time(task.created_at),
        duration(task.created_at, task.stopped_at),
        formatCount(summary.input_rows),
        formatCount(summary.output_rows),
        formatBytes(summary.shuffle_read_bytes),
        formatBytes(summary.shuffle_write_bytes),
        formatBytes(summary.spilled_bytes),
        formatBytes(summary.peak_memory_bytes),
        formatNs(summary.elapsed_compute_ns),
      ]}));
    }

    function workersTable(workers) {
      return table(['Session', 'Worker', 'Status', 'Host', 'Port', 'Duration'], workers.map(worker => [
        escapeHtml(worker.session_id),
        worker.worker_id,
        status(worker.status),
        escapeHtml(worker.host || ''),
        worker.port || '',
        duration(worker.created_at, worker.stopped_at),
      ]));
    }

    function sessionsTable(sessions) {
      return table(['Session', 'User', 'Status', 'Created', 'Deleted'], sessions.map(session => [
        escapeHtml(session.session_id),
        escapeHtml(session.user_id),
        status(session.status),
        time(session.created_at),
        time(session.deleted_at),
      ]));
    }

    function jobDetails(job, session, stages, tasks) {
      if (!job) return '<div class="error muted">No job record.</div>';
      const activeStages = stages.filter(x => !x.stopped_at).length;
      const completedStages = stages.length - activeStages;
      const activeTasks = tasks.filter(x => !x.stopped_at).length;
      const completedTasks = tasks.length - activeTasks;
      return `<div class="kv">
        ${kv('Status', status(job.status))}
        ${kv('Session', escapeHtml(job.session_id))}
        ${kv('Session User', session ? escapeHtml(session.user_id) : '<span class="muted">unknown</span>')}
        ${kv('Session Status', session ? status(session.status) : '<span class="muted">unknown</span>')}
        ${kv('Submitted', time(job.created_at))}
        ${kv('Completed', time(job.stopped_at) || '<span class="muted">running</span>')}
        ${kv('Duration', duration(job.created_at, job.stopped_at))}
        ${kv('Session Created', session ? time(session.created_at) : '')}
        ${kv('Stages', `${completedStages}/${stages.length} completed`)}
        ${kv('Active Stages', activeStages)}
        ${kv('Tasks', `${completedTasks}/${tasks.length} completed, ${activeTasks} active`)}
      </div>`;
    }

    function eventTimeline(stages) {
      const items = [...stages].sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
      if (!items.length) return '<div class="error muted">No stages recorded.</div>';
      const starts = items.map(x => new Date(x.created_at).getTime()).filter(Number.isFinite);
      const stops = items.map(x => x.stopped_at ? new Date(x.stopped_at).getTime() : Date.now()).filter(Number.isFinite);
      const min = Math.min(...starts);
      const max = Math.max(...stops, min + 1);
      return `<div class="timeline">${items.map(stage => {
        const start = new Date(stage.created_at).getTime();
        const stop = stage.stopped_at ? new Date(stage.stopped_at).getTime() : Date.now();
        const left = Math.max(0, ((start - min) / (max - min)) * 100);
        const width = Math.max(0.5, ((stop - start) / (max - min)) * 100);
        const cls = stage.status === 'FAILED' ? 'failed' : (!stage.stopped_at ? 'running' : '');
        return `<div class="timeline-row">
          <span><a href="#" data-job="${stage.job_id}">Stage ${stage.stage}</a></span>
          <div class="timeline-track"><div class="timeline-bar ${cls}" style="left:${left}%;width:${width}%"></div></div>
          <span class="muted">${duration(stage.created_at, stage.stopped_at)}</span>
        </div>`;
      }).join('')}</div>`;
    }

    function summaryMetrics(stages, tasks) {
      const summary = sumSummaries(stages.map(stageSummary));
      const completedTasks = tasks.filter(x => x.stopped_at).length;
      const started = stages.map(x => new Date(x.created_at).getTime()).filter(Number.isFinite);
      const stopped = stages.map(x => x.stopped_at ? new Date(x.stopped_at).getTime() : Date.now()).filter(Number.isFinite);
      const stageDuration = started.length ? duration(Math.min(...started), Math.max(...stopped)) : '';
      const cards = [
        ['Duration', stageDuration],
        ['Succeeded Tasks', completedTasks],
        ['Input Rows', formatCount(summary.input_rows)],
        ['Output Rows', formatCount(summary.output_rows)],
        ['Output Size', formatBytes(summary.output_bytes)],
        ['Shuffle Read', formatBytes(summary.shuffle_read_bytes)],
        ['Shuffle Write', formatBytes(summary.shuffle_write_bytes)],
        ['Spill', formatBytes(summary.spilled_bytes)],
        ['Peak Memory', formatBytes(summary.peak_memory_bytes)],
        ['Executor Compute Time', formatNs(summary.elapsed_compute_ns)],
        ['Join Time', formatNs(summary.join_time_ns)],
        ['Build Time', formatNs(summary.build_time_ns)],
      ];
      return `<div class="metrics-grid">${cards.map(([label, value]) => metricSmall(value || '0', label)).join('')}</div>`;
    }

    function taskMetricsSummary(tasks) {
      if (!tasks.length) return '<div class="error muted">No task metrics recorded.</div>';
      const rows = [
        ['Duration', tasks.map(taskDurationMs), formatMs],
        ['Executor Compute Time', tasks.map(task => taskSummary(task).elapsed_compute_ns), formatNs],
        ['Input Rows', tasks.map(task => taskSummary(task).input_rows), formatCount],
        ['Output Rows', tasks.map(task => taskSummary(task).output_rows), formatCount],
        ['Shuffle Read', tasks.map(task => taskSummary(task).shuffle_read_bytes), formatBytes],
        ['Shuffle Write', tasks.map(task => taskSummary(task).shuffle_write_bytes), formatBytes],
        ['Spill', tasks.map(task => taskSummary(task).spilled_bytes), formatBytes],
        ['Peak Memory', tasks.map(task => taskSummary(task).peak_memory_bytes), formatBytes],
      ].map(([name, values, formatter]) => {
        const clean = values.map(Number).filter(Number.isFinite);
        return [
          escapeHtml(name),
          formatter(quantile(clean, 0)),
          formatter(quantile(clean, 0.25)),
          formatter(quantile(clean, 0.5)),
          formatter(quantile(clean, 0.75)),
          formatter(quantile(clean, 1)),
        ];
      });
      return table(['Metric', 'Min', '25th percentile', 'Median', '75th percentile', 'Max'], rows);
    }

    function operatorMetricsSections(stages) {
      const sections = [...stages].sort((a, b) => a.stage - b.stage).map(stage => {
        const metrics = parseMetrics(stage.metrics_json);
        const taskMetrics = metrics.tasks || [];
        const operators = taskMetrics.flatMap(task => (task.metrics?.operators || []).map(operator => ({
          partition: task.partition,
          attempt: task.attempt,
          status: task.status,
          ...operator,
        })));
        const fallback = metrics.plan?.operators || [];
        const rows = (operators.length ? operators : fallback).flatMap(operator =>
          (operator.metrics || []).map(item => [
            stage.stage,
            operator.partition ?? '<span class="muted">plan</span>',
            operator.attempt ?? '',
            escapeHtml(operator.name),
            escapeHtml(item.name),
            escapeHtml(formatMetricValue(item)),
          ])
        );
        return `
          <details class="metric-details" ${rows.length ? '' : 'open'}>
            <summary>Stage ${stage.stage} Accumulators</summary>
            ${table(['Stage', 'Partition', 'Attempt', 'Operator', 'Metric', 'Value'], rows)}
          </details>
        `;
      }).join('');
      return sections || '<div class="error muted">No operator metrics recorded.</div>';
    }

    function stageSummary(stage) {
      return normalizeSummary(parseMetrics(stage.metrics_json).summary || {});
    }

    function taskSummary(task) {
      return normalizeSummary(parseMetrics(task.metrics_json).summary || {});
    }

    function sumSummaries(items) {
      const total = normalizeSummary({});
      items.forEach(item => {
        Object.keys(total).forEach(key => {
          if (key === 'peak_memory_bytes') total[key] = Math.max(total[key], Number(item[key] || 0));
          else total[key] += Number(item[key] || 0);
        });
      });
      return total;
    }

    function normalizeSummary(value) {
      const keys = [
        'output_rows', 'output_bytes', 'output_batches', 'input_rows', 'input_batches',
        'spilled_bytes', 'spilled_rows', 'spill_count', 'peak_memory_bytes',
        'elapsed_compute_ns', 'join_time_ns', 'build_time_ns',
        'shuffle_read_rows', 'shuffle_read_bytes', 'shuffle_write_rows', 'shuffle_write_bytes',
      ];
      return Object.fromEntries(keys.map(key => [key, Number(value?.[key] || 0)]));
    }

    function parseMetrics(value) {
      if (!value) return {};
      try { return JSON.parse(value); } catch (_) { return {}; }
    }

    function stageDag(stages) {
      if (!stages.length) return '<span class="muted">No stages recorded for this job.</span>';
      const sorted = [...stages].sort((a, b) => a.stage - b.stage);
      const levels = stageLevels(sorted);
      const nodeW = 190;
      const nodeH = 76;
      const gapX = 84;
      const gapY = 32;
      const positions = new Map();
      const grouped = new Map();
      sorted.forEach(stage => {
        const level = levels.get(stage.stage) || 0;
        if (!grouped.has(level)) grouped.set(level, []);
        grouped.get(level).push(stage);
      });
      [...grouped.entries()].forEach(([level, items]) => {
        items.sort((a, b) => a.stage - b.stage).forEach((stage, index) => {
          positions.set(stage.stage, {
            x: 24 + level * (nodeW + gapX),
            y: 24 + index * (nodeH + gapY),
          });
        });
      });
      const width = Math.max(520, 48 + (Math.max(...levels.values(), 0) + 1) * nodeW + Math.max(...levels.values(), 0) * gapX);
      const height = Math.max(180, 48 + Math.max(...[...positions.values()].map(p => p.y + nodeH), nodeH));
      const edges = [];
      sorted.forEach(stage => {
        (stage.inputs || []).forEach(input => {
          if (positions.has(input.stage)) edges.push({ from: input.stage, to: stage.stage, mode: input.mode });
        });
      });
      return `
        <svg class="dag-svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Stage DAG">
          <defs>
            <marker id="arrow" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
              <path d="M0,0 L10,4 L0,8 z" fill="#31708f"></path>
            </marker>
          </defs>
          ${edges.map(edge => dagEdge(edge, positions, nodeW, nodeH)).join('')}
          ${sorted.map(stage => dagNode(stage, positions.get(stage.stage), nodeW, nodeH)).join('')}
        </svg>
      `;
    }

    function physicalOperatorDag(stages) {
      const withPlans = [...stages]
        .sort((a, b) => a.stage - b.stage)
        .filter(stage => stage.physical_plan);
      if (!withPlans.length) return '<div class="error muted">No physical plans recorded for this job.</div>';
      return `<div class="operator-grid">${withPlans.map(stage => `
        <div class="operator-panel">
          <h3>Stage ${stage.stage} Operators</h3>
          <div class="operator-scroll">${operatorTreeSvg(stage)}</div>
        </div>
      `).join('')}</div>`;
    }

    function physicalPlanText(stages) {
      const withPlans = [...stages]
        .sort((a, b) => a.stage - b.stage)
        .filter(stage => stage.physical_plan);
      if (!withPlans.length) return '<div class="error muted">No physical plans recorded for this job.</div>';
      return withPlans.map(stage => `
        <details class="plan-text">
          <summary>Stage ${stage.stage} physical plan text</summary>
          <pre>${escapeHtml(numberedPhysicalPlan(stage))}</pre>
        </details>
      `).join('');
    }

    function numberedPhysicalPlan(stage) {
      return parsePhysicalPlan(stage.physical_plan).map(node => {
        const prefix = node.depth === 0 ? '' : `${'  '.repeat(Math.max(0, node.depth - 1))}+- `;
        return `${prefix}${operatorTextWithId(node)}`;
      }).join('\n');
    }

    function operatorTreeSvg(stage) {
      const nodes = parsePhysicalPlan(stage.physical_plan);
      if (!nodes.length) return '<div class="error muted">No operator nodes parsed for this stage.</div>';
      const nodeW = 360;
      const nodeH = 138;
      const gapX = 28;
      const gapY = 70;
      attachOperatorMetrics(nodes, stage);
      const maxDepth = Math.max(...nodes.map(x => x.depth), 0);
      const levels = new Map();
      nodes.forEach(node => {
        if (!levels.has(node.depth)) levels.set(node.depth, []);
        levels.get(node.depth).push(node);
      });
      [...levels.entries()].forEach(([depth, items]) => {
        items.forEach((node, index) => {
          node.x = 18 + index * (nodeW + gapX);
          node.y = 18 + (maxDepth - depth) * (nodeH + gapY);
        });
      });
      const maxLevelWidth = Math.max(...[...levels.values()].map(items => items.length), 1);
      const width = Math.max(620, 36 + maxLevelWidth * nodeW + Math.max(0, maxLevelWidth - 1) * gapX);
      const height = Math.max(220, 36 + (maxDepth + 1) * nodeH + maxDepth * gapY);
      const edges = nodes.filter(node => node.parent != null).map(node => ({ from: node, to: nodes[node.parent] }));
      return `
        <svg class="operator-svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Stage ${stage.stage} physical operators">
          <defs>
            <marker id="operator-arrow-${stage.stage}" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
              <path d="M0,0 L10,4 L0,8 z" fill="#31708f"></path>
            </marker>
          </defs>
          ${edges.map(edge => operatorEdge(edge, nodeW, nodeH, stage.stage)).join('')}
          ${nodes.map(node => operatorNode(node, nodeW, nodeH)).join('')}
        </svg>
      `;
    }

    function parsePhysicalPlan(plan) {
      const nodes = [];
      const stack = [];
      String(plan || '').split('\n').forEach(line => {
        if (!line.trim()) return;
        const prefix = line.match(/^[\s│├└─]*/)[0] || '';
        const leading = prefix.replace(/[├└─]/g, ' ').length;
        const trimmed = line.trim().replace(/^[├└─\s]+/, '');
        const nameMatch = trimmed.match(/^([A-Za-z][A-Za-z0-9_]*(?:Exec|Scan|Sink|Source|Query|Table|Adapter|Reader|Writer)?)/);
        const name = nameMatch ? nameMatch[1] : trimmed.split(/[:\s]/)[0];
        if (!name) return;
        const depth = Math.max(0, Math.floor(leading / 2));
        while (stack.length && stack[stack.length - 1].depth >= depth) stack.pop();
        const parent = stack.length ? stack[stack.length - 1].index : null;
        const detail = trimmed.slice(name.length).replace(/^[:\s]+/, '').trim();
        if (isMetricWrapperOperator(name)) {
          stack.push({ depth, index: parent });
          return;
        }
        const index = nodes.length;
        const visibleDepth = stack.filter(item => item.index != null).length;
        const node = { index, depth: visibleDepth, parent, name, detail, raw: trimmed };
        nodes.push(node);
        stack.push({ depth, index });
      });
      return assignOperatorIndexes(
        collapseDuplicatePlanWrappers(nodes).map((node, index) => ({ ...node, index }))
      );
    }

    function assignOperatorIndexes(nodes) {
      const children = new Map();
      nodes.forEach(node => {
        if (node.parent == null) return;
        if (!children.has(node.parent)) children.set(node.parent, []);
        children.get(node.parent).push(node.index);
      });
      const order = [];
      const visit = index => {
        order.push(index);
        (children.get(index) || []).forEach(visit);
      };
      nodes.filter(node => node.parent == null).forEach(node => visit(node.index));
      const orderMap = new Map(order.map((index, orderIndex) => [index, orderIndex]));
      const displayOrder = [];
      const displayVisited = new Set();
      const visitForDisplay = index => {
        if (displayVisited.has(index)) return;
        displayVisited.add(index);
        (children.get(index) || []).forEach(visitForDisplay);
        displayOrder.push(index);
      };
      nodes.forEach(node => visitForDisplay(node.index));
      const displayOrderMap = new Map(displayOrder.map((index, orderIndex) => [index, orderIndex]));
      return nodes.map(node => ({
        ...node,
        displayIndex: (displayOrderMap.get(node.index) ?? node.index) + 1,
        metricIndex: orderMap.get(node.index) ?? node.index,
      }));
    }

    function collapseDuplicatePlanWrappers(nodes) {
      const duplicateParents = new Set();
      nodes.forEach(node => {
        if (node.parent == null) return;
        const parent = nodes[node.parent];
        if (parent && isDuplicatePlanWrapper(parent, node)) duplicateParents.add(parent.index);
      });
      if (!duplicateParents.size) return nodes;
      const replacement = new Map();
      nodes.forEach(node => {
        if (!duplicateParents.has(node.index)) return;
        const child = nodes.find(candidate => candidate.parent === node.index && isDuplicatePlanWrapper(node, candidate));
        if (child) replacement.set(node.index, child.index);
      });
      const kept = nodes.filter(node => !duplicateParents.has(node.index));
      const indexMap = new Map(kept.map((node, index) => [node.index, index]));
      const normalized = kept.map(node => {
        let parent = node.parent;
        while (parent != null && duplicateParents.has(parent)) {
          const child = replacement.get(parent);
          parent = child === node.index ? nodes[parent].parent : (child ?? nodes[parent].parent);
        }
        const normalizedParent = parent == null ? null : indexMap.get(parent);
        return {
          ...node,
          parent: normalizedParent == null ? null : normalizedParent,
        };
      });
      const memo = new Map();
      function depth(index) {
        if (memo.has(index)) return memo.get(index);
        const parent = normalized[index].parent;
        const value = parent == null ? 0 : depth(parent) + 1;
        memo.set(index, value);
        return value;
      }
      return normalized.map((node, index) => ({ ...node, depth: depth(index) }));
    }

    function isDuplicatePlanWrapper(parent, child) {
      return normalizeOperatorName(parent.name) === normalizeOperatorName(child.name)
        && normalizePlanDetail(parent.detail) === normalizePlanDetail(child.detail);
    }

    function normalizePlanDetail(value) {
      return String(value || '').replace(/\s+/g, ' ').trim();
    }

    function operatorEdge(edge, nodeW, nodeH, stageId) {
      const x1 = edge.from.x + nodeW / 2;
      const y1 = edge.from.y + nodeH;
      const x2 = edge.to.x + nodeW / 2;
      const y2 = edge.to.y;
      const mid = y1 + Math.max(32, Math.abs(y2 - y1) / 2);
      return `<path class="operator-edge" marker-end="url(#operator-arrow-${stageId})" d="M ${x1} ${y1} C ${x1} ${mid}, ${x2} ${mid}, ${x2} ${y2 - 8}"></path>`;
    }

    function operatorNode(node, nodeW, nodeH) {
      const kind = operatorKind(node.name);
      const lines = wrapText(node.detail || node.raw || 'physical operator', 42, 3);
      const metricLines = operatorMetricLines(node, 42, 2);
      const title = `${node.raw}${node.metrics?.length ? '\n' + node.metrics.map(item => `${item.name}: ${formatMetricValue(item)}`).join('\n') : ''}`;
      return `
        <g transform="translate(${node.x}, ${node.y})">
          <rect class="operator-node ${kind}" width="${nodeW}" height="${nodeH}" rx="4"></rect>
          <text class="operator-title" x="12" y="20">${escapeHtml(operatorNameWithId(node))}</text>
          ${lines.map((line, index) => `<text class="operator-meta" x="12" y="${40 + index * 16}">${escapeHtml(line)}</text>`).join('')}
          ${metricLines.map((line, index) => `<text class="operator-metric" x="12" y="${96 + index * 16}">${escapeHtml(line)}</text>`).join('')}
          <title>${escapeHtml(title)}</title>
        </g>
      `;
    }

    function operatorNameWithId(node) {
      return `${node.name} (${node.displayIndex})`;
    }

    function operatorTextWithId(node) {
      const raw = String(node.raw || node.name || '');
      if (raw.startsWith(node.name)) {
        return `${operatorNameWithId(node)}${raw.slice(node.name.length)}`;
      }
      return `${operatorNameWithId(node)} ${raw}`;
    }

    function attachOperatorMetrics(nodes, stage) {
      const metrics = parseMetrics(stage.metrics_json);
      const operators = collectStageOperators(metrics);
      const byId = new Map(operators.filter(x => x.operator_id != null).map(x => [Number(x.operator_id), x]));
      const byName = new Map();
      operators.forEach(operator => {
        const key = normalizeOperatorName(operator.name);
        if (!byName.has(key)) byName.set(key, []);
        byName.get(key).push(operator);
      });
      byName.forEach(candidates => candidates.sort((a, b) => Number(hasMetrics(b)) - Number(hasMetrics(a))));
      nodes.forEach(node => {
        let operator = node.metricIndex == null ? null : byId.get(node.metricIndex);
        if (operator && !hasMetrics(operator)) {
          const candidates = byName.get(normalizeOperatorName(node.name)) || [];
          const better = candidates.find(hasMetrics);
          if (better) operator = better;
        }
        if (!operator) {
          const candidates = byName.get(normalizeOperatorName(node.name)) || [];
          operator = candidates.shift();
        }
        node.metrics = operator?.metrics || [];
        node.hasMetricSource = Boolean(operator);
      });
    }

    function collectStageOperators(metrics) {
      if (!metrics) return [];
      if (Array.isArray(metrics.operators)) return metrics.operators;
      if (Array.isArray(metrics.tasks)) {
        const merged = new Map();
        metrics.tasks.forEach(task => {
          (task.metrics?.operators || []).forEach(operator => {
            const key = operator.operator_id ?? operator.name;
            if (!merged.has(key)) merged.set(key, { ...operator, metrics: [] });
            const target = merged.get(key);
            target.metrics = mergeMetricLists(target.metrics, operator.metrics || []);
          });
        });
        const operators = [...merged.values()];
        if (operators.some(hasMetrics)) return operators;
      }
      if (Array.isArray(metrics.plan?.operators)) return metrics.plan.operators;
      return [];
    }

    function hasMetrics(operator) {
      return Array.isArray(operator?.metrics) && operator.metrics.length > 0;
    }

    function mergeMetricLists(left, right) {
      const merged = new Map((left || []).map(item => [item.name, { ...item }]));
      (right || []).forEach(item => {
        const existing = merged.get(item.name);
        if (!existing) merged.set(item.name, { ...item });
        else {
          existing.value = Number(existing.value || 0) + Number(item.value || 0);
          existing.display = formatMetricValue(existing);
        }
      });
      return [...merged.values()];
    }

    function normalizeOperatorName(name) {
      return String(name || '').replace(/^.*::/, '').replace(/Exec$/, '').toLowerCase();
    }

    function isMetricWrapperOperator(name) {
      return /^(TracingExec|TraceExec)$/i.test(String(name || ''));
    }

    function operatorMetricLines(node, width, maxLines) {
      const metrics = node.metrics || [];
      const items = [...(metrics || [])].sort((a, b) => {
        const rank = metricRank(a) - metricRank(b);
        return rank || String(a.name).localeCompare(String(b.name));
      }).slice(0, maxLines);
      if (!items.length) return [];
      return items.map(item => truncate(`${item.name}: ${formatMetricValue(item)}`, width));
    }

    function metricRank(item) {
      const preferred = ['output_rows', 'bytes_scanned', 'elapsed_compute', 'input_rows', 'output_bytes', 'spilled_bytes', 'mem_used', 'peak_mem_used', 'join_time', 'build_time'];
      const index = preferred.indexOf(item.name);
      if (index >= 0) return index;
      if (item.kind === 'bytes') return 100;
      if (item.kind === 'time') return 120;
      if (item.kind === 'count') return 140;
      if (item.kind === 'gauge') return 160;
      return 200;
    }

    function operatorKind(name) {
      if (/Exchange|Repartition/i.test(name)) return 'exchange';
      if (/Join/i.test(name)) return 'join';
      if (/Filter/i.test(name)) return 'filter';
      if (/Project/i.test(name)) return 'project';
      return '';
    }

    function stageLevels(stages) {
      const byId = new Map(stages.map(stage => [stage.stage, stage]));
      const memo = new Map();
      function level(stageId, stack = new Set()) {
        if (memo.has(stageId)) return memo.get(stageId);
        const stage = byId.get(stageId);
        if (!stage || stack.has(stageId)) return 0;
        stack.add(stageId);
        const inputs = (stage.inputs || []).filter(input => byId.has(input.stage));
        const value = inputs.length ? 1 + Math.max(...inputs.map(input => level(input.stage, stack))) : 0;
        stack.delete(stageId);
        memo.set(stageId, value);
        return value;
      }
      stages.forEach(stage => level(stage.stage));
      return memo;
    }

    function dagEdge(edge, positions, nodeW, nodeH) {
      const a = positions.get(edge.from);
      const b = positions.get(edge.to);
      const x1 = a.x + nodeW;
      const y1 = a.y + nodeH / 2;
      const x2 = b.x;
      const y2 = b.y + nodeH / 2;
      const mid = x1 + Math.max(32, (x2 - x1) / 2);
      return `<path class="dag-edge" d="M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2 - 8} ${y2}"><title>Stage ${edge.from} to Stage ${edge.to} (${escapeHtml(edge.mode)})</title></path>`;
    }

    function dagNode(stage, pos, nodeW, nodeH) {
      const cls = String(stage.status || '').toLowerCase();
      const inputText = (stage.inputs || []).map(x => `${x.stage} ${x.mode}`).join(', ') || 'source';
      const summary = stageSummary(stage);
      const metricText = `${formatCount(summary.output_rows)} rows · ${formatNs(summary.elapsed_compute_ns)}`;
      return `
        <g transform="translate(${pos.x}, ${pos.y})">
          <rect class="dag-node ${escapeHtml(cls)}" width="${nodeW}" height="${nodeH}" rx="4"></rect>
          <circle class="dag-badge" cx="18" cy="20" r="6"></circle>
          <text class="dag-title" x="32" y="24">Stage ${stage.stage}</text>
          <text class="dag-meta" x="14" y="46">${escapeHtml(stage.status)} · ${stage.partitions} partitions</text>
          <text class="dag-meta" x="14" y="64">${escapeHtml(truncate(metricText, 28))}</text>
          <title>Stage ${stage.stage}: ${escapeHtml(stage.status)}, ${stage.partitions} partitions, inputs ${escapeHtml(inputText)}, output ${escapeHtml(formatCount(summary.output_rows))}, compute ${escapeHtml(formatNs(summary.elapsed_compute_ns))}</title>
        </g>
      `;
    }

    function pageTitle(title, detail) {
      return `<div class="page-title"><h2>${escapeHtml(title)}</h2><span class="muted">${escapeHtml(detail || '')}</span></div>`;
    }

    function titleForView(view) {
      return ({ jobs: 'Jobs', stages: 'Stages', tasks: 'Tasks', workers: 'Executors' })[view] || 'Jobs';
    }

    function routeFromLocation() {
      const parts = window.location.pathname.split('/').filter(Boolean);
      const query = new URLSearchParams(window.location.search);
      const result = {
        view: query.get('view') || 'jobs',
        jobId: query.has('jobId') ? Number(query.get('jobId')) : null,
        appId: null,
      };
      if (parts[0] === 'history') {
        result.appId = parts[1] ? decodeURIComponent(parts[1]) : null;
        result.view = viewFromPathSegment(parts[2]) || result.view;
      } else {
        result.view = viewFromPathSegment(parts[0]) || result.view;
      }
      if (!['jobs', 'stages', 'tasks', 'workers'].includes(result.view)) result.view = 'jobs';
      if (!Number.isFinite(result.jobId)) result.jobId = null;
      return result;
    }

    function viewFromPathSegment(segment) {
      return ({ jobs: 'jobs', stages: 'stages', tasks: 'tasks', executors: 'workers', workers: 'workers' })[segment] || null;
    }

    function routeForView(view) {
      const segment = view === 'workers' ? 'executors' : view;
      const query = state.selectedJob == null ? '' : `?jobId=${encodeURIComponent(state.selectedJob)}`;
      if (state.appId) return `/history/${encodeURIComponent(state.appId)}/${segment}/${query}`;
      return `/?view=${encodeURIComponent(view)}${state.selectedJob == null ? '' : `&jobId=${encodeURIComponent(state.selectedJob)}`}`;
    }

    function pushRoute() {
      const next = routeForView(state.view);
      if (next !== `${window.location.pathname}${window.location.search}`) {
        history.pushState(null, '', next);
      }
    }

    function activateNav() {
      buttons.forEach(button => button.classList.toggle('active', button.dataset.view === state.view));
    }

    function appHeader(sessions) {
      const active = sessions.filter(session => !session.deleted_at && session.status !== 'DELETED');
      const first = [...sessions].sort((a, b) => new Date(a.created_at) - new Date(b.created_at))[0];
      const users = [...new Set(sessions.map(session => session.user_id).filter(Boolean))].join(', ') || 'unknown';
      const appName = state.appId ? `Sail Application ${escapeHtml(state.appId)}` : 'Sail Application';
      return `<div class="app-header">
        <h1>${appName}</h1>
        <div class="app-meta">
          <div>User: ${escapeHtml(users)}</div>
          <div>Active Sessions: ${active.length} / ${sessions.length}${first ? ` · Started: ${time(first.created_at)}` : ''}</div>
        </div>
      </div>`;
    }

    function subnav(view) {
      const items = [
        ['jobs', 'Jobs'],
        ['stages', 'Stages'],
        ['tasks', 'Tasks'],
        ['workers', 'Executors'],
      ];
      return `<div class="subnav">${items.map(([id, label]) => `<a href="${routeForView(id)}" data-view-link="${id}" class="${id === view ? 'active' : ''}">${label}</a>`).join('')}</div>`;
    }

    function tableSection(title, body) {
      return `<section><h2>${escapeHtml(title)}</h2>${body}</section>`;
    }

    function table(headers, rows) {
      if (!rows.length) return '<div class="error muted">No rows.</div>';
      return `<table><thead><tr>${headers.map(x => `<th>${escapeHtml(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
    }

    function metric(value, label) {
      return `<div class="metric"><strong>${value}</strong><span>${escapeHtml(label)}</span></div>`;
    }

    function metricSmall(value, label) {
      return `<div class="metric small"><strong>${escapeHtml(value)}</strong><span>${escapeHtml(label)}</span></div>`;
    }

    function kv(label, value) {
      return `<div><span>${escapeHtml(label)}</span>${value}</div>`;
    }

    function status(value) {
      return `<span class="status ${escapeHtml(value || '')}">${escapeHtml(value || '')}</span>`;
    }

    function time(value) {
      return value ? escapeHtml(new Date(value).toLocaleString()) : '';
    }

    function duration(start, stop) {
      const ms = durationValue(start, stop);
      if (!Number.isFinite(ms)) return '';
      return formatMs(ms);
    }

    function durationValue(start, stop) {
      if (!start) return NaN;
      const end = stop ? new Date(stop) : new Date();
      return Math.max(0, end - new Date(start));
    }

    function taskDurationMs(task) {
      return durationValue(task.created_at, task.stopped_at);
    }

    function formatMs(ms) {
      if (!Number.isFinite(ms)) return '';
      if (ms < 1000) return `${ms} ms`;
      if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
      return `${(ms / 60000).toFixed(1)} min`;
    }

    function quantile(values, q) {
      if (!values.length) return 0;
      const sorted = [...values].sort((a, b) => a - b);
      const index = (sorted.length - 1) * q;
      const lo = Math.floor(index);
      const hi = Math.ceil(index);
      if (lo === hi) return sorted[lo];
      return sorted[lo] + (sorted[hi] - sorted[lo]) * (index - lo);
    }

    function formatMetricValue(item) {
      if (item.kind === 'bytes') return formatBytes(item.value);
      if (item.kind === 'time') return formatNs(item.value);
      return item.display || formatCount(item.value);
    }

    function formatCount(value) {
      const number = Number(value || 0);
      return number.toLocaleString();
    }

    function formatBytes(value) {
      const number = Number(value || 0);
      if (number < 1024) return `${number.toLocaleString()} B`;
      const units = ['KB', 'MB', 'GB', 'TB'];
      let current = number / 1024;
      let unit = 0;
      while (current >= 1024 && unit < units.length - 1) {
        current /= 1024;
        unit += 1;
      }
      return `${current.toFixed(current >= 10 ? 1 : 2)} ${units[unit]}`;
    }

    function formatNs(value) {
      const ns = Number(value || 0);
      if (ns < 1000000) return `${Math.round(ns / 1000).toLocaleString()} us`;
      if (ns < 1000000000) return `${(ns / 1000000).toFixed(1)} ms`;
      return `${(ns / 1000000000).toFixed(2)} s`;
    }

    function truncate(value, length) {
      const text = String(value ?? '');
      return text.length > length ? `${text.slice(0, Math.max(0, length - 1))}…` : text;
    }

    function wrapText(value, width, maxLines) {
      const text = String(value ?? '').replace(/\s+/g, ' ').trim();
      if (!text) return ['physical operator'];
      const lines = [];
      let rest = text;
      while (rest && lines.length < maxLines) {
        if (rest.length <= width) {
          lines.push(rest);
          break;
        }
        let cut = rest.lastIndexOf(' ', width);
        if (cut < Math.floor(width * 0.6)) cut = width;
        lines.push(rest.slice(0, cut).trim());
        rest = rest.slice(cut).trim();
      }
      if (rest && lines.length) lines[lines.length - 1] = truncate(lines[lines.length - 1], Math.max(4, width - 1));
      return lines;
    }

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }

    load();
  </script>
</body>
</html>"##;
