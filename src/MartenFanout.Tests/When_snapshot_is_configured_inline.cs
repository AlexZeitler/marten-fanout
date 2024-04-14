using Marten;
using Marten.Events.Aggregation;
using Marten.Events.Projections;
using MartenFanout.Tests.TestSetup;

namespace MartenFanout.Tests;

public record SomethingHappened(DateTimeOffset On);

public record Something(DateTimeOffset On)
{
  public Guid Id { get; set; }
};

public class SomethingProjection : SingleStreamProjection<Something>
{
  public static Something Create(
    SomethingHappened happened
  ) =>
    new(happened.On);
}

[TestFixture]
public class When_projection_is_configured_inline
{
  public class When_event_is_persisted
  {
    private Guid _streamId;
    private IDocumentStore? _store;
    private TestEventStore _testEventStore;

    [SetUp]
    public async Task InitializeAsync()
    {
      _testEventStore = await TestEventStore.InitializeAsync(
        options => { options.Projections.Add<SomethingProjection>(ProjectionLifecycle.Inline); }
      );

      _store = _testEventStore.Store;

      var on = DateTimeOffset.Now;
      var happened = new SomethingHappened(on);
      _streamId = Guid.NewGuid();

      await using var session = _store.LightweightSession();
      session.Events.Append(_streamId, happened);
      await session.SaveChangesAsync();
    }

    [Test]
    public async Task should_write_projection_in_same_transaction()
    {
      await using var session = _store?.QuerySession();
      var something = await session?.LoadAsync<Something>(_streamId)!;
      something.ShouldNotBeNull();
    }

    [TearDown]
    public async Task DisposeAsync() => await _testEventStore.DisposeAsync();
  }
}
