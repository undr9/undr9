# Debug Session: reconnect-missing-nodes
- **Status**: [OPEN]
- **Issue**: Newly created nodes are not visible after refresh or reconnect.
- **Debug Server**: Pending initialization
- **Log File**: `.dbg/trae-debug-log-reconnect-missing-nodes.ndjson`

## Reproduction Steps
1. Open the UNDR9 UI workspace in a scoped namespace.
2. Create a new node from the query modal.
3. Confirm the node appears immediately.
4. Refresh the graph or reconnect to the workspace.
5. Observe that the new node is no longer visible.

## Hypotheses
| ID | Hypothesis | Likelihood | Effort | Evidence |
|----|------------|------------|--------|----------|
| A | `discoverGraph()` does not return the inserted node after reconnect. | High | Low | Pending |
| B | Store hydration or reconnect flow overwrites fresh graph state with stale cached state. | High | Medium | Pending |
| C | Reconnect uses a different namespace/profile than the insert flow. | Medium | Low | Pending |
| D | Discovery merge or namespace scoping drops disconnected inserted nodes. | High | Low | Pending |
| E | Canvas render/layout receives the node but fails to present it after reconnect. | Medium | Medium | Pending |

## Evidence Log
- Pending instrumentation

## Conclusion
- Pending
