using AsyncKeyedLock;
using EventDriven.DDD.Abstractions.Entities;
using EventDriven.Sagas.Abstractions.Commands;
using EventDriven.Sagas.Abstractions.Dispatchers;
using EventDriven.Sagas.Abstractions.Evaluators;
using EventDriven.Sagas.Abstractions.Handlers;
using EventDriven.Sagas.Abstractions.Pools;

namespace EventDriven.Sagas.Abstractions;

/// <summary>
/// Enables the execution of atomic operations which span multiple services.
/// </summary>
public abstract class Saga
{
    private readonly AsyncNonKeyedLocker _syncRoot = new();

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="sagaCommandDispatcher">Saga command dispatcher.</param>
    /// <param name="commandResultEvaluators">Command result evaluators.</param>
    /// <param name="sagaPool">Saga pool.</param>
    protected Saga(
        ISagaCommandDispatcher sagaCommandDispatcher,
        IEnumerable<ISagaCommandResultEvaluator> commandResultEvaluators,
        ISagaPool sagaPool)
    {
        SagaCommandDispatcher = sagaCommandDispatcher;
        CommandResultEvaluators = commandResultEvaluators;
        SagaPool = sagaPool;
    }

    /// <summary>
    /// Lock timeout for synchronizing multi-threaded access.
    /// </summary>
    protected TimeSpan LockTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Cancellation token.
    /// </summary>
    protected CancellationToken CancellationToken;

    /// <summary>
    /// Saga identifier.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Time the saga was started.
    /// </summary>
    public DateTime Started { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Saga snapshot sequence.
    /// </summary>
    public int Sequence { get; set; }

    /// <summary>
    /// Entity identifier.
    /// </summary>
    public Guid EntityId { get; set; }

    /// <summary>
    /// Entity.
    /// </summary>
    public IEntity? Entity { get; set; }

    /// <summary>
    /// The current saga step.
    /// </summary>
    public int CurrentStep { get; set; }

    /// <summary>
    /// State of the saga.
    /// </summary>
    public SagaState State { get; set; }

    /// <summary>
    /// Information about the state of the saga.
    /// </summary>
    public string? StateInfo { get; set; }

    /// <summary>
    /// True if saga is locked.
    /// </summary>
    public bool IsLocked { get; set; }

    /// <summary>
    /// If true override lock check.
    /// </summary>
    public bool OverrideLockCheck { get; set; }
    
    /// <summary>
    /// Enable saga snapshots. Defaults to true.
    /// </summary>
    public bool EnableSagaSnapshots { get; set; } = true;

    /// <summary>
    /// Represents a unique ID that must change atomically with each store of the entity
    /// to its underlying storage medium.
    /// </summary>
    public string ETag { get; set; } = null!;

    /// <summary>
    /// Steps performed by the saga.
    /// </summary>
    public List<SagaStep> Steps { get; set; } = new();

    /// <summary>
    /// Check lock command handler.
    /// </summary>
    public ICheckSagaLockCommandHandler? CheckLockCommandHandler { get; set; }

    /// <summary>
    /// Saga command dispatcher.
    /// </summary>
    protected ISagaCommandDispatcher SagaCommandDispatcher { get; set; }

    /// <summary>
    /// Command result evaluators.
    /// </summary>
    protected IEnumerable<ISagaCommandResultEvaluator> CommandResultEvaluators { get; }

    /// <summary>
    /// Saga pool.
    /// </summary>
    protected ISagaPool SagaPool { get; }

    /// <summary>
    /// Check if saga is locked.
    /// </summary>
    /// <param name="entityId">Entity identifier.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected abstract Task<bool> CheckLock(Guid entityId);

    /// <summary>
    /// Hook for executing code after each step.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected virtual Task ExecuteAfterStep() => Task.CompletedTask;

    /// <summary>
    /// Execute the current action.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected virtual async Task ExecuteCurrentActionAsync()
    {
        var action = GetCurrentAction();
        SetActionStateStarted(action);
        SetActionCommand(action);
        await SagaCommandDispatcher.DispatchCommandAsync(action.Command, false);
    }

    /// <summary>
    /// Execute the current compensating action.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected virtual async Task ExecuteCurrentCompensatingActionAsync()
    {
        var action = GetCurrentCompensatingAction();
        SetActionStateStarted(action);
        SetActionCommand(action);
        await SagaCommandDispatcher.DispatchCommandAsync(action.Command, true);
    }

    /// <summary>
    /// Get command result evaluator for this saga by result type.
    /// </summary>
    /// <typeparam name="TSaga">Saga type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <typeparam name="TExpectedResult">Expected result type.</typeparam>
    /// <returns>ISagaCommandResultEvaluator for this saga with the specified result type.</returns>
    protected virtual ISagaCommandResultEvaluator<TResult, TExpectedResult>? GetCommandResultEvaluatorByResultType
        <TSaga, TResult, TExpectedResult>()
        where TSaga : Saga
    {
        return CommandResultEvaluators
            .Where(e => e.SagaType == null || e.SagaType == typeof(TSaga))
            .OfType<ISagaCommandResultEvaluator<TResult, TExpectedResult>>().FirstOrDefault();
    }

    /// <summary>
    /// Handle command result for step.
    /// </summary>
    /// <param name="compensating">True if compensating step.</param>
    /// <typeparam name="TSaga">Saga type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <typeparam name="TExpectedResult">Expected result type.</typeparam>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected virtual async Task HandleCommandResultForStepAsync<TSaga, TResult, TExpectedResult>(bool compensating)
        where TSaga : Saga
    {
        var step = Steps.Single(s => s.Sequence == CurrentStep);
        var evaluator = GetCommandResultEvaluatorByResultType<TSaga, TResult, TExpectedResult>();
        var commandSuccessful = evaluator != null
            && await EvaluateStepResultAsync(step, compensating, evaluator, CancellationToken);
        await ExecuteAfterStep();
        await TransitionSagaStateAsync(commandSuccessful);
    }

    /// <summary>
    /// Get current action.
    /// </summary>
    /// <returns>The current action.</returns>
    protected virtual SagaAction GetCurrentAction()
    {
        var action = Steps.Single(s => s.Sequence == CurrentStep).Action;
        action.Command.SagaId = Id;
        return action;
    }

    /// <summary>
    /// Get current compensating action.
    /// </summary>
    /// <returns>The current compensating action.</returns>
    protected virtual SagaAction GetCurrentCompensatingAction()
    {
        var action = Steps.Single(s => s.Sequence == CurrentStep).CompensatingAction;
        action.Command.SagaId = Id;
        return action;
    }

    /// <summary>
    /// Set action state and started time.
    /// </summary>
    /// <param name="action">Saga action.</param>
    protected virtual void SetActionStateStarted(SagaAction action)
    {
        action.State = ActionState.Running;
        action.Started = DateTime.UtcNow;
    }

    /// <summary>
    /// Set action command.
    /// </summary>
    /// <param name="action">Saga action.</param>
    /// <param name="commandEntity">Command entity.</param>
    protected virtual void SetActionCommand(SagaAction action, IEntity? commandEntity = null) => 
        action.Command = commandEntity != null
        ? action.Command with { Entity = commandEntity, EntityId = commandEntity.Id }
        : action.Command with { EntityId = EntityId };

    /// <summary>
    /// Set current action command result.
    /// </summary>
    /// <param name="result">Result.</param>
    /// <param name="compensating">True if compensating action.</param>
    /// <typeparam name="TResult">Result type.</typeparam>
    protected virtual void SetCurrentActionCommandResult<TResult>(TResult result, bool compensating)
    {
        var step = Steps.Single(s => s.Sequence == CurrentStep);
        var action = compensating ? step.CompensatingAction : step.Action;
        if (action.Command is SagaCommand<TResult, TResult> command)
            command.Result = result;
    }
    
    /// <summary>
    /// Evaluate a step result.
    /// </summary>
    /// <param name="step">Saga step.</param>
    /// <param name="compensating">True if compensating step.</param>
    /// <param name="evaluator">Saga command result evaluator.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <typeparam name="TExpectedResult">Expected result type.</typeparam>
    /// <returns>
    /// A task that represents the asynchronous operation.
    /// The task result contains a boolean that is true if step completed successfully.
    /// </returns>
    public virtual async Task<bool> EvaluateStepResultAsync<TResult, TExpectedResult>(
        SagaStep step, bool compensating, ISagaCommandResultEvaluator<TResult, TExpectedResult> evaluator,
        CancellationToken cancellationToken)
    {
        // Evaluate result
        var isCancelled = !compensating && cancellationToken.IsCancellationRequested;
        var action = compensating ? step.CompensatingAction : step.Action;
        if (action.Command is not ISagaCommand<TResult, TExpectedResult> command) return false;
        var result = command.Result;
        var expectedResult = command.ExpectedResult;
        var commandSuccessful = !isCancelled
            && await evaluator.EvaluateCommandResultAsync(command.Result, command.ExpectedResult);

        // Check timeout
        action.Completed = DateTime.UtcNow;
        action.Duration = action.Completed - action.Started;
        var duration = action.Duration;
        var timeout = action.Timeout;
        var commandTimedOut = commandSuccessful && action.Timeout != null && action.Duration > action.Timeout;
        if (commandTimedOut) commandSuccessful = false;

        // Transition action state
        action.State = ActionState.Succeeded;
        if (!commandSuccessful)
        {
            if (isCancelled)
            {
                action.State = ActionState.Cancelled;
                action.StateInfo = GetCancellationMessage();
            }
            else if (!commandTimedOut)
            {
                action.State = ActionState.Failed;
                action.StateInfo = GetFailureMessage(result, expectedResult);
            }
            else
            {
                action.State = ActionState.TimedOut;
                action.StateInfo = GetTimeoutMessage(timeout, duration);
            }

            var commandName = action.Command.Name ?? "No name";
            StateInfo = $"Step {step.Sequence} command '{commandName}' failed. {action.StateInfo}";
            return false;
        }
        return true;
    }
    
    /// <summary>
    /// Get cancellation message.
    /// </summary>
    /// <returns>The cancellation message.</returns>
    protected virtual string GetCancellationMessage() => "Cancellation requested.";
    
    /// <summary>
    /// Get failure message.
    /// </summary>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <typeparam name="TExpectedResult">Expected result type.</typeparam>
    /// <returns>The failure message.</returns>
    protected virtual string GetFailureMessage<TResult, TExpectedResult>(
        TResult result, TExpectedResult expectedResult) =>
        result == null || expectedResult == null
            ? "Unexpected result."
            : $"'{result}' returned when '{expectedResult}' was expected.";
    
    /// <summary>
    /// Get timeout message.
    /// </summary>
    /// <param name="timeout">Set timeout.</param>
    /// <param name="duration">Step duration.</param>
    /// <returns>Timeout message.</returns>
    protected virtual string GetTimeoutMessage(
        TimeSpan? timeout ,TimeSpan? duration) =>
        timeout == null || duration == null
            ? "Duration exceeded timeout."
            : $"Duration of '{duration.Value:c}' exceeded timeout of '{timeout.Value:c}'";

    private readonly AsyncLocal<int> _recursionCount = new();

    /// <summary>
    /// Transition saga state.
    /// </summary>
    /// <param name="commandSuccessful">True if command was successful.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected virtual async Task TransitionSagaStateAsync(bool commandSuccessful)
    {
        try
        {
            using (await _syncRoot.ConditionalLockAsync(++_recursionCount.Value == 1))
            {
                var reverseOnFailure = GetCurrentAction().ReverseOnFailure;
                switch (State)
                {
                    case SagaState.Executing:
                        if (CurrentStep < Steps.Max(s => s.Sequence))
                        {
                            if (commandSuccessful)
                            {
                                CurrentStep++;
                                await SagaPool.ReplaceSagaAsync(this);
                                await ExecuteCurrentActionAsync();
                            }
                            else
                            {
                                if (!reverseOnFailure) CurrentStep--;
                                State = SagaState.Compensating;
                                await SagaPool.ReplaceSagaAsync(this);
                                await ExecuteCurrentCompensatingActionAsync();
                            }
                        }
                        else
                        {
                            State = commandSuccessful ? SagaState.Executed : SagaState.Compensating;
                            if (!commandSuccessful)
                            {
                                if (!reverseOnFailure) CurrentStep--;
                                await SagaPool.ReplaceSagaAsync(this);
                                await ExecuteCurrentCompensatingActionAsync();
                            }
                            else
                            {
                                await SagaPool.RemoveSagaAsync(Id);
                            }
                        }
                        return;
                    case SagaState.Compensating:
                        if (!commandSuccessful) // Exit if compensating action unsuccessful
                            return;
                        if (CurrentStep > Steps.Min(s => s.Sequence))
                        {
                            CurrentStep--;
                            await SagaPool.ReplaceSagaAsync(this);
                            await ExecuteCurrentCompensatingActionAsync();
                        }
                        else
                        {
                            State = SagaState.Compensated;
                            await SagaPool.RemoveSagaAsync(Id);
                        }
                        return;
                    default:
                        return;
                }
            }
        }
        finally
        {
            --_recursionCount.Value;
        }
    }

    /// <summary>
    /// Start the saga.
    /// </summary>
    /// <param name="entityId">Entity identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="SagaLockedException">Thrown when the saga is locked.</exception>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public virtual async Task StartSagaAsync(Guid entityId = default, CancellationToken cancellationToken = default)
    {
        if (!OverrideLockCheck) IsLocked = await CheckLock(entityId);
        if (IsLocked) throw new SagaLockedException($"Saga '{Id}' is currently locked.");

        // Set state, current step, entity id, cancellation token
        State = SagaState.Executing;
        CurrentStep = 1;
        EntityId = entityId;
        CancellationToken = cancellationToken;
        
        // Save saga
        await SagaPool.ReplaceSagaAsync(this);

        // Dispatch current step command
        await ExecuteCurrentActionAsync();
    }

    /// <summary>
    /// Start the saga.
    /// </summary>
    /// <param name="entity">Entity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public virtual async Task StartSagaAsync(IEntity entity, CancellationToken cancellationToken = default)
    {
        Entity = entity;
        await StartSagaAsync(entity.Id, cancellationToken);
    }
}

/// <inheritdoc />
public abstract class Saga<TMetadata> : Saga
    where TMetadata : class
{
    /// <summary>
    /// Saga metadata.
    /// </summary>
    public TMetadata? Metadata { get; set; }

    /// <inheritdoc />
    protected Saga(ISagaCommandDispatcher sagaCommandDispatcher,
        IEnumerable<ISagaCommandResultEvaluator> commandResultEvaluators,
        ISagaPool sagaPool) :
        base(sagaCommandDispatcher, commandResultEvaluators, sagaPool)
    {
    }
    
    /// <summary>
    /// Start the saga.
    /// </summary>
    /// <param name="entity">Entity.</param>
    /// <param name="metadata">Saga metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public virtual async Task StartSagaAsync(IEntity entity, TMetadata metadata,
        CancellationToken cancellationToken = default)
    {
        Metadata = metadata;
        Entity = entity;
        await StartSagaAsync(entity.Id, cancellationToken);
    }
}