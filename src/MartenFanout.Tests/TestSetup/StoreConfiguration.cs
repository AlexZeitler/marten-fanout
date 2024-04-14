using Marten;
using Weasel.Core;

namespace MartenFanout.Tests.TestSetup;

public static class StoreConfiguration
{
  public static StoreOptions Configure(
    StoreOptions options
  )
  {
    options.AutoCreateSchemaObjects = AutoCreate.All;
    return options;
  }
}
