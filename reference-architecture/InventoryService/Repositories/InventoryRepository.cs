using AsyncKeyedLock;
using EventDriven.DDD.Abstractions.Repositories;
using InventoryService.Domain.InventoryAggregate;
using MongoDB.Driver;
using URF.Core.Mongo;

namespace InventoryService.Repositories;

public class InventoryRepository : DocumentRepository<Inventory>, IInventoryRepository
{
    private readonly AsyncNonKeyedLocker _syncRoot = new();

    public InventoryRepository(
        IMongoCollection<Inventory> collection) : base(collection)
    {
    }
    
    public async Task<IEnumerable<Inventory>> GetAsync() =>
        await FindManyAsync();

    public async Task<Inventory?> GetAsync(Guid id) =>
        await FindOneAsync(e => e.Id == id);

    public async Task<Inventory?> AddAsync(Inventory entity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await FindOneAsync(e => e.Id == entity.Id);
            if (existing != null) return null;
            entity.ETag = Guid.NewGuid().ToString();
            return await InsertOneAsync(entity);
        }
    }

    public async Task<Inventory?> UpdateAsync(Inventory entity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(entity.Id);
            if (existing == null) return null;
            if (string.Compare(entity.ETag, existing.ETag, StringComparison.OrdinalIgnoreCase) != 0)
                throw new ConcurrencyException();
            entity.ETag = Guid.NewGuid().ToString();
            return await FindOneAndReplaceAsync(e => e.Id == entity.Id, entity);
        }
    }

    public async Task<int> RemoveAsync(Guid id) => 
        await DeleteOneAsync(e => e.Id == id);
}