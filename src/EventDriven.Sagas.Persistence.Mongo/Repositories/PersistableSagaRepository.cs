using AsyncKeyedLock;
using AutoMapper;
using EventDriven.Sagas.Persistence.Abstractions;
using EventDriven.Sagas.Persistence.Abstractions.DTO;
using EventDriven.Sagas.Persistence.Abstractions.Repositories;
using MongoDB.Driver;
using System.Text.Json;
using URF.Core.Mongo;

namespace EventDriven.Sagas.Persistence.Mongo.Repositories;

/// <summary>
/// Persistable saga repository.
/// </summary>
public class PersistableSagaRepository<TSaga> : 
    DocumentRepository<PersistableSagaDto>, IPersistableSagaRepository<TSaga>
    where TSaga : PersistableSaga
{
    private readonly IMapper _mapper;
    private readonly AsyncNonKeyedLocker _syncRoot = new();

    /// <summary>
    /// PersistableSagaRepository constructor.
    /// </summary>
    /// <param name="collection">IMongoCollection.</param>
    /// <param name="mapper">Auto mapper.</param>
    public PersistableSagaRepository(
        IMongoCollection<PersistableSagaDto> collection,
        IMapper mapper) : base(collection)
    {
        _mapper = mapper;
    }

    /// <inheritdoc />
    public async Task<TSaga?> GetAsync(Guid id, TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            var dto = await FindOneAsync(e => e.SagaId == id);
            if (dto is null) return null;
            _mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public async Task<TSaga> CreateAsync(TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            newEntity.ETag = Guid.NewGuid().ToString();
            var dto = _mapper.Map<PersistableSagaDto>(newEntity);
            dto.Id = Guid.NewGuid();
            await InsertOneAsync(dto);
            _mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public async Task<TSaga> SaveAsync(TSaga existingEntity, TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(existingEntity.Id, newEntity);
            if (existing is null)
                return await CreateAsync(newEntity);

            existingEntity.ETag = Guid.NewGuid().ToString();
            var dto = _mapper.Map<PersistableSagaDto>(existingEntity);
            dto.Id = Guid.NewGuid();
            await RemoveAsync(existingEntity.Id);
            await InsertOneAsync(dto);
            _mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public async Task RemoveAsync(Guid id) => 
        await DeleteOneAsync(e => e.SagaId == id);
}

/// <summary>
/// Persistable saga repository with metadata.
/// </summary>
/// <typeparam name="TSaga">Saga</typeparam>
/// <typeparam name="TMetaData">Metadata</typeparam>
public class PersistableSagaRepository<TSaga, TMetaData> :
    DocumentRepository<PersistableSagaMetadataDto>, IPersistableSagaRepository<TSaga, TMetaData>
    where TSaga : PersistableSaga<TMetaData> 
    where TMetaData : class
{
    private readonly IMapper _mapper;
    private readonly AsyncNonKeyedLocker _syncRoot = new();

    /// <summary>
    /// PersistableSagaRepository constructor.
    /// </summary>
    /// <param name="collection">IMongoCollection.</param>
    /// <param name="mapper">Auto mapper.</param>
    public PersistableSagaRepository(
        IMongoCollection<PersistableSagaMetadataDto> collection,
        IMapper mapper) : base(collection)
    {
        _mapper = mapper;
    }

    /// <inheritdoc />
    public async Task<TSaga?> GetAsync(Guid id, TSaga newEntity)
    {
        var dto = await FindOneAsync(e => e.SagaId == id);
        if (dto is null) return null;
        _mapper.Map(dto, newEntity);
        var metadata = JsonSerializer.Deserialize<TMetaData>(dto.Metadata);
        newEntity.Metadata = metadata;
        return newEntity;
    }

    /// <inheritdoc />
    public async Task<TSaga> CreateAsync(TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            newEntity.ETag = Guid.NewGuid().ToString();
            var dto = _mapper.Map<PersistableSagaMetadataDto>(newEntity);
            dto.Id = Guid.NewGuid();
            await InsertOneAsync(dto);
            _mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public async Task<TSaga> SaveAsync(TSaga existingEntity, TSaga newEntity)
    {
        using (await _syncRoot.LockAsync())
        {
            var existing = await GetAsync(existingEntity.Id, newEntity);
            if (existing is null)
                return await CreateAsync(newEntity);

            existingEntity.ETag = Guid.NewGuid().ToString();
            var dto = _mapper.Map<PersistableSagaMetadataDto>(existingEntity);
            dto.Id = Guid.NewGuid();
            await RemoveAsync(existingEntity.Id);
            await InsertOneAsync(dto);
            _mapper.Map(dto, newEntity);
            return newEntity;
        }
    }

    /// <inheritdoc />
    public async Task RemoveAsync(Guid id) => 
        await DeleteOneAsync(e => e.SagaId == id);
}