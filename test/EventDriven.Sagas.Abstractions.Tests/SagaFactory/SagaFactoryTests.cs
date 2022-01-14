using System.Threading.Tasks;
using EventDriven.Sagas.Abstractions.Commands;
using EventDriven.Sagas.Abstractions.Entities;
using EventDriven.Sagas.Abstractions.Factories;
using EventDriven.Sagas.Abstractions.Tests.SagaFactory.Fakes;
using EventDriven.Sagas.Configuration.Abstractions;
using EventDriven.Sagas.Configuration.Abstractions.Factories;
using EventDriven.Sagas.Persistence.Abstractions.Factories;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventDriven.Sagas.Abstractions.Tests.SagaFactory;

public class SagaFactoryTests
{
    [Theory]
    [InlineData(SagaType.Basic)]
    [InlineData(SagaType.Configurable)]
    [InlineData(SagaType.Persistable)]
    public async Task SagaFactoryShouldCreateConfigurableSaga(SagaType sagaType)
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<FakeSagaCommandDispatcher>();
        services.AddSingleton<FakeSagaCommandHandler>();
        services.AddSingleton<ISagaCommandResultEvaluator, FakeCommandResultEvaluator>();
        services.AddSingleton<ISagaCommandHandler, FakeSagaCommandHandler>(sp =>
            sp.GetRequiredService<FakeSagaCommandHandler>());
        services.AddSingleton<ISagaCommandResultDispatcher, FakeSagaCommandHandler>(sp =>
            sp.GetRequiredService<FakeSagaCommandHandler>());
        services.AddSingleton(sp =>
        {
            var sagaConfigOptions = new SagaConfigurationOptions();
            var dispatcher = sp.GetRequiredService<FakeSagaCommandDispatcher>();
            var evaluators = sp.GetServices<ISagaCommandResultEvaluator>();
            var resultDispatchers = sp.GetServices<ISagaCommandResultDispatcher>();
            ISagaFactory<Entities.Saga> factory;
            switch (sagaType)
            {
                case SagaType.Configurable:
                    factory = new ConfigurableSagaFactory<FakeConfigurableSaga>(
                        dispatcher, evaluators, resultDispatchers, 
                        sagaConfigOptions, null!);
                    break;
                case SagaType.Persistable:
                    factory = new PersistableSagaFactory<FakePersistableSaga>(
                        dispatcher, evaluators, resultDispatchers,
                        sagaConfigOptions, null!, null!);
                    break;
                default:
                    factory = new SagaFactory<FakeSaga>(
                        dispatcher, evaluators, resultDispatchers);
                    break;
            }
            return factory;
        });
        services.AddSingleton(sp =>
        {
            var factory = sp.GetRequiredService<ISagaFactory<Entities.Saga>>();
            var saga = factory.CreateSaga();
            return saga;
        });

        // Act
        var provider = services.BuildServiceProvider();
        var saga = provider.GetRequiredService<Entities.Saga>();
        await saga.StartSagaAsync();

        // Assert
        var expectedState = sagaType == SagaType.Basic ? SagaState.Executing : SagaState.Executed;
        var expectedInfo = sagaType == SagaType.Basic ? null : FakeSaga.SuccessState;
        Assert.Equal(expectedState, saga.State);
        Assert.Equal(expectedInfo, saga.StateInfo);
    }
}