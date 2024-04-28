using AsyncKeyedLock;
using EventDriven.DDD.Abstractions.Repositories;
using MongoDB.Driver;
using OrderService.Domain.OrderAggregate;
using URF.Core.Mongo;

namespace OrderService.Repositories;

public class OrderRepository : DocumentRepository<Order>, IOrderRepository
{
    private readonly AsyncNonKeyedLocker _syncRoot = new();

    public OrderRepository(
        IMongoCollection<Order> collection) : base(collection)
    {
    }

    public async Task<IEnumerable<Order>> GetOrders() =>
        await FindManyAsync();

    public async Task<IEnumerable<Order>> GetCustomerOrders(Guid customerId) =>
        await FindManyAsync(e => e.CustomerId == customerId);

    public async Task<Order?> GetAsync(Guid id) =>
        await FindOneAsync(e => e.Id == id);

    public async Task<Order?> AddOAsync(Order entity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(entity.Id);
            if (existing != null) throw new ConcurrencyException(entity.Id);
            entity.ETag = Guid.NewGuid().ToString();
            return await InsertOneAsync(entity);
        }
    }

    public async Task<Order?> UpdateAsync(Order entity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(entity.Id);
            if (existing == null) return null;
            if (string.Compare(entity.ETag, existing.ETag, StringComparison.OrdinalIgnoreCase) != 0)
                throw new ConcurrencyException(entity.Id);
            entity.ETag = Guid.NewGuid().ToString();
            return await FindOneAndReplaceAsync(e => e.Id == entity.Id, entity);
        }
    }

    public async Task<Order?> AddUpdateAsync(Order entity)
    {
        using (await _syncRoot.LockAsync())
        {
            Order? result;
            var existing = await GetAsync(entity.Id);
            if (existing == null) result = await AddOAsync(entity);
            else
            {
                entity.ETag = existing.ETag;
                result = await UpdateAsync(entity);
            }

            return result;
        }
    }

    public async Task<int> RemoveAsync(Guid id) =>
        await DeleteOneAsync(e => e.Id == id);

    public async Task<OrderState?> GetOrderStateAsync(Guid id)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(id);
            return existing?.State;
        }
    }

    public async Task<Order?> UpdateOrderStateAsync(Guid id, OrderState orderState)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(id);
            if (existing == null) return null;
            existing.State = orderState;
            return await UpdateAsync(existing);
        }
    }
}