import { IExecutor } from "./Executor";
import ITask from "./Task";

export default async function run(
  executor: IExecutor,
  queue: AsyncIterable<ITask>,
  maxThreads = 0
) {
  maxThreads = Math.max(0, maxThreads);

  // Track running tasks per targetId and overall running count
  const runningTasks = new Map<number, Promise<void>>();
  const targetQueues = new Map<number, ITask[]>();
  let runningCount = 0;
  let queueIterator: AsyncIterator<ITask> | null =
    queue[Symbol.asyncIterator]();
  let queueDone = false;
  let consecutiveEmptyChecks = 0;

  // Function to execute a task and handle the continuation
  async function executeTask(task: ITask): Promise<void> {
    try {
      await executor.executeTask(task);
    } finally {
      runningCount--;
      runningTasks.delete(task.targetId);

      // Continue with next task for this targetId if available
      const tasks = targetQueues.get(task.targetId);
      if (tasks && tasks.length > 0) {
        if (maxThreads === 0 || runningCount < maxThreads) {
          const nextTask = tasks.shift()!;
          runningCount++;
          const promise = executeTask(nextTask);
          runningTasks.set(task.targetId, promise);
        }
      } else {
        // Clean up empty queue
        targetQueues.delete(task.targetId);
      }
    }
  }

  // Function to try starting tasks that can run
  function tryStartTasks(): void {
    for (const [targetId, tasks] of targetQueues.entries()) {
      if (
        !runningTasks.has(targetId) &&
        tasks.length > 0 &&
        (maxThreads === 0 || runningCount < maxThreads)
      ) {
        const task = tasks.shift()!;
        runningCount++;
        const promise = executeTask(task);
        runningTasks.set(targetId, promise);
      }
    }
  }

  // Function to load one task from queue
  async function loadOneTask(): Promise<ITask | null> {
    if (queueDone || !queueIterator) {
      return null;
    }

    const result = await queueIterator.next();

    if (result.done) {
      return null; // Don't mark as done yet, queue might get more tasks
    }

    consecutiveEmptyChecks = 0; // Reset counter when we find a task
    return result.value;
  }

  // Main processing loop
  while (true) {
    // Try to load and queue tasks when we have available capacity
    while (
      !queueDone &&
      (maxThreads === 0 || runningCount < maxThreads || targetQueues.size === 0)
    ) {
      const task = await loadOneTask();
      if (!task) break;

      // Add task to target-specific queue
      if (!targetQueues.has(task.targetId)) {
        targetQueues.set(task.targetId, []);
      }
      targetQueues.get(task.targetId)!.push(task);

      // Try to start this task immediately if possible
      if (
        !runningTasks.has(task.targetId) &&
        (maxThreads === 0 || runningCount < maxThreads)
      ) {
        const taskToRun = targetQueues.get(task.targetId)!.shift()!;
        runningCount++;
        const promise = executeTask(taskToRun);
        runningTasks.set(task.targetId, promise);
      }

      // If we have thread limits and are at capacity, stop loading for now
      if (maxThreads > 0 && runningCount >= maxThreads) {
        break;
      }
    }

    // Try to start any queued tasks that can now run
    tryStartTasks();

    // If we have running tasks, wait for one to complete
    if (runningTasks.size > 0) {
      await Promise.race(runningTasks.values());
      continue;
    }

    // If no tasks are running but we have queued tasks, something is wrong
    if (targetQueues.size > 0) {
      // Force start one task
      const firstTargetId = targetQueues.keys().next().value;
      const tasks = targetQueues.get(firstTargetId)!;
      if (tasks.length > 0) {
        const task = tasks.shift()!;
        runningCount++;
        const promise = executeTask(task);
        runningTasks.set(firstTargetId, promise);
        continue;
      }
    }

    // If nothing is running or queued, check if we should wait for more tasks
    if (runningTasks.size === 0 && targetQueues.size === 0) {
      // Try to load one more task
      const task = await loadOneTask();
      if (task) {
        // New task found, add it and continue
        if (!targetQueues.has(task.targetId)) {
          targetQueues.set(task.targetId, []);
        }
        targetQueues.get(task.targetId)!.push(task);
        continue;
      } else {
        // No new tasks found, increment counter
        consecutiveEmptyChecks++;

        // For dynamic queues, be more patient before giving up
        if (consecutiveEmptyChecks >= 5) {
          queueDone = true;
          break;
        }

        // Give the queue a small delay to potentially add more tasks
        await new Promise((resolve) => setTimeout(resolve, 1));
        continue;
      }
    }
  }
}
