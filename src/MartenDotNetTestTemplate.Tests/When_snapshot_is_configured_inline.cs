using Marten;
using Marten.Events.Aggregation;
using Marten.Events.Projections;
using static MartenDotNetTestTemplate.Tests.TestDatabase;

namespace MartenDotNetTestTemplate.Tests;

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
    private DocumentStore? _store;

    [SetUp]
    public async Task InitializeAsync()
    {
      _store = DocumentStore.For(
        _ =>
        {
          _.Connection(GetTestDbConnectionString());
          _.Projections.Add<SomethingProjection>(ProjectionLifecycle.Inline);
        }
      );

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
    public async Task DisposeAsync() => await _store.DisposeAsync();
  }
}
