using Npgsql;

namespace MartenDotNetTestTemplate.Tests;

public class TestDatabase
{
  public static string GetTestDbConnectionString() =>
    new NpgsqlConnectionStringBuilder()
    {
      Pooling = false,
      Port = 5435,
      Host = "localhost",
      CommandTimeout = 20,
      Database = "postgres",
      Password = "123456",
      Username = "postgres"
    }.ToString();
}
