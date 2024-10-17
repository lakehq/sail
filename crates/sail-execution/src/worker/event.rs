pub enum WorkerEvent {
    ServerReady {
        /// The local port that the server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
    },
    Shutdown,
}
