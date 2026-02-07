# Background Hooks Design - Issue #1041

## Problem Statement

Currently, churn execution waits for the entire job to complete before starting. For large workloads (115 iterations), this creates significant delays. The request is to enable **concurrent** background execution.

## Current Flow

```go
// job.go lines 129-142
RunCreateJob(ctx, 0, jobIterations)  // Wait for ALL iterations
if config.IsChurnEnabled() {
    RunCreateJobWithChurn(ctx)        // Then start churn
}
```

## Proposed Solution

### Option 1: Threshold-Based Background Start (Recommended)

Add configuration to start background processes after a threshold of iterations complete.

#### Configuration Changes

```yaml
jobs:
  - name: rds-core
    jobIterations: 115
    churnConfig:
      mode: namespaces
      cycles: 10
      percent: 10
      delay: 1m
      # NEW: Start churn after N iterations complete
      startAfterIterations: 20  # Start churn after 20 iterations
```

#### Implementation

```go
// In types.go - add to ChurnConfig
type ChurnConfig struct {
    Cycles               int
    Percent              int  
    Duration             time.Duration
    Delay                time.Duration
    Mode                 ChurnMode
    StartAfterIterations int `yaml:"startAfterIterations"`  // NEW
}

// In job.go - modify execution flow
if config.IsChurnEnabled(jobExecutor.Job) {
    var churnWg sync.WaitGroup
    churnCtx, churnCancel := context.WithCancel(ctx)
    defer churnCancel()
    
    // Start churn in background after threshold
    churnWg.Add(1)
    go func() {
        defer churnWg.Done()
        
        // Wait for threshold iterations
        threshold := jobExecutor.ChurnConfig.StartAfterIterations
        if threshold <= 0 {
            threshold = jobExecutor.JobIterations // Default: wait for all
        }
        
        // Poll until threshold reached
        for {
            select {
            case <-churnCtx.Done():
                return
            case <-time.After(5 * time.Second):
                if atomic.LoadInt32(&jobExecutor.completedIterations) >= int32(threshold) {
                    goto startChurn
                }
            }
        }
        
    startChurn:
        churnStart := time.Now().UTC()
        executedJobs[jobExecutorIdx].ChurnStart = &churnStart
        jobExecutor.RunCreateJobWithChurn(churnCtx)
        churnEnd := time.Now().UTC()
        executedJobs[jobExecutorIdx].ChurnEnd = &churnEnd
    }()
    
    // Main job continues
    if jobErrs := jobExecutor.RunCreateJob(ctx, 0, jobExecutor.JobIterations); jobErrs != nil {
        errs = append(errs, jobErrs...)
    }
    
    // Wait for churn to complete
    churnWg.Wait()
}
```

### Option 2: Generic Background Hooks (More Complex)

Add generic pre/post hooks that can run in background.

```yaml
jobs:
  - name: rds-core
    backgroundHooks:
      - name: "churn-background"
        startAfterIterations: 20
        command: "internal:churn"  # Built-in churn
      - name: "custom-script"
        startAfterIterations: 50
        command: "/path/to/script.sh"
```

## Recommendation

**Option 1** is simpler and addresses the immediate use case. It:
- Requires minimal code changes
- Maintains backward compatibility (default behavior unchanged)
- Solves the specific problem described in #1041

**Option 2** is more flexible but adds complexity for uncertain future needs.

## Implementation Steps

1. Add `StartAfterIterations` to `ChurnConfig` in `types.go`
2. Add `completedIterations` atomic counter to `JobExecutor`
3. Modify job execution flow to start churn in goroutine
4. Add synchronization to wait for background churn
5. Add tests for concurrent execution
6. Update documentation

## Backward Compatibility

- Default value `0` means "wait for all iterations" (current behavior)
- Existing configs work unchanged
