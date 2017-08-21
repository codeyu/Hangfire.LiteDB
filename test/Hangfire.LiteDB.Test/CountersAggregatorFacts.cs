using System;
using System.Threading;
using Hangfire.LiteDB.Entities;
using Hangfire.LiteDB.Test.Utils;
using Xunit;

namespace Hangfire.LiteDB.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class CountersAggregatorFacts
    {
        [Fact, CleanDatabase]
        public void CountersAggregatorExecutesProperly()
        {
            var storage = ConnectionUtils.CreateStorage();
            using (var connection = (LiteDbConnection)storage.GetConnection())
            {
                // Arrange
                connection.Database.StateDataCounter.Insert(new Counter
                {
                    Key = "key",
                    Value = 1L,
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });

                var aggregator = new CountersAggregator(storage, TimeSpan.Zero);
                var cts = new CancellationTokenSource();
                cts.Cancel();

                // Act
                aggregator.Execute(cts.Token);

                // Assert
                Assert.Equal(1, connection.Database.StateDataAggregatedCounter.Count());
            }
        }
    }
#pragma warning restore 1591
}