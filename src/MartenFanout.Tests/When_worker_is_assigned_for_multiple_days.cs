using Marten;
using Marten.Events.Aggregation;
using Marten.Events.Projections;
using MartenFanout.Tests.TestSetup;

namespace MartenFanout.Tests;

public record WorkerAssigned(Guid AssignmentId, Guid WorkerId, DateOnly Start, DateOnly End);

public record WorkCompleted(Guid AssignmentId, DateOnly Date, string DescriptionOfWork);

public record WorkByDay(string Id, Guid AssignmentId, Guid WorkerId, DateOnly Date, string[] WorkCompleted);

public record WorkerAssignedForDay(Guid AssignmentId, Guid WorkerId, DateOnly Date);

public record WorkerAssignment(Guid AssignmentId, Guid WorkerId, DateOnly Start, DateOnly End)
{
  public Guid Id { get; set; }
}

public class WorkByDayProjection : MultiStreamProjection<WorkByDay, string>
{
  public WorkByDayProjection()
  {
    FanOut<WorkerAssigned, WorkerAssignedForDay>(
      e => Enumerable.Range(0, e.End.DayNumber - e.Start.DayNumber + 1)
        .Select(
          d => new WorkerAssignedForDay(
            e.AssignmentId,
            e.WorkerId,
            e.Start.AddDays(d)
          )
        ),
      FanoutMode.BeforeGrouping
    );
    Identity<WorkerAssignedForDay>(e => IdForEvent(e.AssignmentId, e.Date));
    Identity<WorkCompleted>(e => IdForEvent(e.AssignmentId, e.Date));
    IncludeType<WorkerAssigned>();
  }

  private static string IdForEvent(
    Guid assignmentId,
    DateOnly date
  ) => $"{assignmentId:N}:{date:yyyyMMdd}";

  public static WorkByDay Create(
    WorkerAssignedForDay evt
  ) => new(
    IdForEvent(evt.AssignmentId, evt.Date),
    evt.AssignmentId,
    evt.WorkerId,
    evt.Date,
    Array.Empty<string>()
  );

  public static WorkByDay Apply(
    WorkCompleted evt,
    WorkByDay view
  ) =>
    view with
    {
      WorkCompleted = view.WorkCompleted.Append(evt.DescriptionOfWork)
        .ToArray()
    };
}

public class WorkerAssignmentProjection : SingleStreamProjection<WorkerAssignment>
{
  public static WorkerAssignment Create(
    WorkerAssigned evt
  ) =>
    new(
      evt.AssignmentId,
      evt.WorkerId,
      evt.Start,
      evt.End
    );
}

[TestFixture]
public class When_worker_is_assigned_for_multiple_days
{
  private IDocumentStore _store;
  private TestEventStore _testEventStore;
  private Guid _workerId;

  [SetUp]
  public async Task InitializeAsync()
  {
    _testEventStore = await TestEventStore.InitializeAsync(
      options =>
      {
        options.Projections.Add<WorkByDayProjection>(ProjectionLifecycle.Inline);
        options.Projections.Add<WorkerAssignmentProjection>(ProjectionLifecycle.Inline);
      }
    );
    _store = _testEventStore.Store;

    var assignmentId = Guid.NewGuid();
    _workerId = Guid.NewGuid();
    var today = DateOnly.FromDateTime(DateTime.Today);
    var tomorrow = today.AddDays(1);
    var oneWeekFromToday = today.AddDays(7);

    await using var session = _store.LightweightSession();

    session.Events.Append(
      assignmentId,
      new WorkerAssigned(
        assignmentId,
        _workerId,
        today,
        oneWeekFromToday
      )
    );
    session.Events.Append(
      assignmentId,
      new WorkCompleted(
        assignmentId,
        today,
        "Work completed today"
      )
    );
    session.Events.Append(
      assignmentId,
      new WorkCompleted(
        assignmentId,
        today,
        "More work completed today"
      )
    );
    session.Events.Append(
      assignmentId,
      new WorkCompleted(
        assignmentId,
        tomorrow,
        "Work completed tomorrow"
      )
    );

    await session.SaveChangesAsync();
  }

  [Test]
  public void should_create_assignment_projection_for_each_day()
  {
    using var session = _store.QuerySession();
    var workByDay = session.Query<WorkByDay>()
      .ToList();
    workByDay.Count.ShouldBe(8);
  }

  [Test]
  public void should_create_assignment_projection()
  {
    using var session = _store.QuerySession();
    var workerAssignment = session.Query<WorkerAssignment>()
      .FirstOrDefault(a => a.WorkerId == _workerId);
    workerAssignment.ShouldNotBeNull();
    workerAssignment.Start.ShouldBe(DateOnly.FromDateTime(DateTime.Today));
    workerAssignment.End.ShouldBe(DateOnly.FromDateTime(DateTime.Today.AddDays(7)));
  }

  [TearDown]
  public async Task DisposeAsync() => await _testEventStore.DisposeAsync();
}
