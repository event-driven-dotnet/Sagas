using System.Text.Json;
using EventDriven.Sagas.Configuration.Abstractions;
using EventDriven.Sagas.Configuration.Abstractions.DTO;
using Integration.Models;
using OrderService.Domain.OrderAggregate;
using OrderService.Sagas.Commands;

namespace SagaConfigDefinitions;

public class CreateOrderSagaConfigDefinition : ISagaConfigDefinition
{
    public SagaConfigurationDto CreateSagaConfig(Guid id)
    {
        var steps = new List<SagaStepDto>
        {
            new SagaStepDto
            {
                Sequence = 1,
                Action = new SagaActionDto
                {
                    Command = JsonSerializer.Serialize(new SagaCommandDto<OrderState>
                    {
                        Name = typeof(CreateOrder).FullName,
                        ExpectedResult = OrderState.Pending
                    }),
                    ReverseOnFailure = true
                },
                CompensatingAction = new SagaActionDto
                {
                    Command = JsonSerializer.Serialize(new SagaCommandDto<OrderState>
                    {
                        Name = typeof(SetOrderStateInitial).FullName,
                        ExpectedResult = OrderState.Initial
                    })
                }
            },
            new SagaStepDto
            {
                Sequence = 2,
                Action = new SagaActionDto
                {
                    Command = JsonSerializer.Serialize(new SagaCommandDto<CustomerCreditReserveResponse>
                    {
                        Name = typeof(ReserveCustomerCredit).FullName,
                        ExpectedResult = null
                    })
                },
                CompensatingAction = new SagaActionDto
                {
                    Command = JsonSerializer.Serialize(new SagaCommandDto<CustomerCreditReleaseResponse>
                    {
                        Name = typeof(ReleaseCustomerCredit).FullName,
                        ExpectedResult = null
                    })
                }
            },
            new SagaStepDto
            {
                Sequence = 3,
                Action = new SagaActionDto
                {
                    Command = JsonSerializer.Serialize(new SagaCommandDto<OrderState>
                    {
                        Name = typeof(SetOrderStateCreated).FullName,
                        ExpectedResult = OrderState.Created
                    }),
                    ReverseOnFailure = true
                },
                CompensatingAction = new SagaActionDto
                {
                    Command = JsonSerializer.Serialize(new SagaCommandDto<OrderState>
                    {
                        Name = typeof(SetOrderStateInitial).FullName,
                        ExpectedResult = OrderState.Initial
                    })
                }
            }
        };
        return new SagaConfigurationDto() { Id = id, Steps = steps, Name = "CreateOrderSaga"};
    }
}