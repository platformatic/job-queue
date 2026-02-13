# Development Guidelines

## Resource Management

- **Always close all resources properly**: Timers (setInterval, setTimeout), event listeners, connections, and file handles must be cleaned up in `disconnect()` or `stop()` methods
- **Make connect/disconnect idempotent**: Multiple calls to `connect()` should not create duplicate resources. Check if already connected before creating intervals or other resources
- **Watch for stuck tests**: If CI hangs after tests complete, it's usually a lingering timer or event listener keeping the process alive. Never use `unref()` as a workaround - find and fix the actual resource leak
- **Test cleanup in afterEach**: Ensure `afterEach` hooks properly stop all queues and disconnect all storage instances

## Testing

- Tests should exit cleanly without `--test-force-exit`
- If a test takes longer than expected, investigate resource cleanup
- The `visibilityTimeout` setting affects how long `stop()` waits for active jobs
