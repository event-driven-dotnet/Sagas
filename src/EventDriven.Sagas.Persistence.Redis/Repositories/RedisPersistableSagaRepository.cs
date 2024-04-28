using AsyncKeyedLock;
using AutoMapper;
using EventDriven.Sagas.Persistence.Abstractions;
using EventDriven.Sagas.Persistence.Abstractions.DTO;
using EventDriven.Sagas.Persistence.Abstractions.Repositories;
using Microsoft.Extensions.Caching.Distributed;
using System.Text.Json;

namespace EventDriven.Sagas.Persistence.Redis.Repositories;

/// <summary>
/// Persistable saga repository.
/// </summary>
public class RedisPersistableSagaRepository<TSaga> : IPersistableSagaRepository<TSaga>
    where TSaga : PersistableSaga
{
    private readonly AsyncNonKeyedLocker _syncRoot = new();

    /// <summary>
    /// Distributed cache.
    /// </summary>
    protected readonly IDistributedCache Cache;
    
    /// <summary>
    /// Distributed cache entry options.
    /// </summary>
    protected readonly DistributedCacheEntryOptions CacheOptions;
    
    /// <summary>
    /// Auto mapper.
    /// </summary>
    protected readonly IMapper Mapper;

    /// <summary>
    /// PersistableSagaRepository constructor.
    /// </summary>
    /// <param name="cache">Distributed cache.</param>
    /// <param name="cacheOptions">Distributed cache entry options.</param>
    /// <param name="mapper">Auto mapper.</param>
    public RedisPersistableSagaRepository(IDistributedCache cache,
        DistributedCacheEntryOptions cacheOptions,
        IMapper mapper)
    {
        Cache = cache;
        CacheOptions = cacheOptions;
        Mapper = mapper;
    }

    /// <inheritdoc />
    public virtual async Task<TSaga?> GetAsync(Guid id, TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            var json = await Cache.GetStringAsync(id.ToString());
            if (json is null) return null;
            var dto = JsonSerializer.Deserialize<PersistableSagaDto>(json);
            Mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public virtual async Task<TSaga> CreateAsync(TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            newEntity.ETag = Guid.NewGuid().ToString();
            var dto = Mapper.Map<PersistableSagaDto>(newEntity);
            dto.Id = Guid.NewGuid();
            await InsertAsync(dto);
            Mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public virtual async Task<TSaga> SaveAsync(TSaga existingEntity, TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(existingEntity.Id, newEntity);
            if (existing is null)
                return await CreateAsync(newEntity);

            existingEntity.ETag = Guid.NewGuid().ToString();
            var dto = Mapper.Map<PersistableSagaDto>(existingEntity);
            dto.Id = Guid.NewGuid();
            await RemoveAsync(existingEntity.Id);
            await InsertAsync(dto);
            Mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public virtual Task RemoveAsync(Guid id) =>
        Cache.RemoveAsync(id.ToString());
    
    private Task InsertAsync(PersistableSagaDto dto)
    {
        var json = JsonSerializer.Serialize(dto);
        return Cache.SetStringAsync(dto.SagaId.ToString(), json, CacheOptions);
    }
}

/// <summary>
/// Persistable saga repository with metadata.
/// </summary>
/// <typeparam name="TSaga">Saga</typeparam>
/// <typeparam name="TMetaData">Metadata</typeparam>
public class RedisPersistableSagaRepository<TSaga, TMetaData> :
    RedisPersistableSagaRepository<TSaga>,
    IPersistableSagaRepository<TSaga, TMetaData>
    where TSaga : PersistableSaga<TMetaData>
    where TMetaData : class
{
    private readonly AsyncNonKeyedLocker _syncRoot = new();

    /// <inheritdoc />
    public RedisPersistableSagaRepository(IDistributedCache cache,
        DistributedCacheEntryOptions cacheOptions, IMapper mapper) :
        base(cache, cacheOptions, mapper)
    {
    }
    
    /// <inheritdoc />
    public override async Task<TSaga?> GetAsync(Guid id, TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            var json = await Cache.GetStringAsync(id.ToString());
            if (json is null) return null;
            var dto = JsonSerializer.Deserialize<PersistableSagaMetadataDto>(json);
            if (dto is null) return null;
            Mapper.Map(dto, newEntity);
            var metadata = JsonSerializer.Deserialize<TMetaData>(dto.Metadata);
            newEntity.Metadata = metadata;
            return newEntity;
        }
    }
    
    /// <inheritdoc />
    public override async Task<TSaga> CreateAsync(TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            newEntity.ETag = Guid.NewGuid().ToString();
            var dto = Mapper.Map<PersistableSagaMetadataDto>(newEntity);
            dto.Id = Guid.NewGuid();
            await InsertAsync(dto);
            Mapper.Map(dto, newEntity);
            return newEntity;
        }
    }
    
    /// <inheritdoc />
    public override async Task<TSaga> SaveAsync(TSaga existingEntity, TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(existingEntity.Id, newEntity);
            if (existing is null)
                return await CreateAsync(newEntity);

            existingEntity.ETag = Guid.NewGuid().ToString();
            var dto = Mapper.Map<PersistableSagaMetadataDto>(existingEntity);
            dto.Id = Guid.NewGuid();
            await RemoveAsync(existingEntity.Id);
            await InsertAsync(dto);
            Mapper.Map(dto, newEntity);
            return newEntity;
        }
    }
    
    private Task InsertAsync(PersistableSagaMetadataDto dto)
    {
        var json = JsonSerializer.Serialize(dto);
        return Cache.SetStringAsync(dto.SagaId.ToString(), json, CacheOptions);
    }
}