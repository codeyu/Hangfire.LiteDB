using System;
using System.Threading;
using Hangfire.LiteDB.Test.Utils;
using LiteDB;
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
                connection.Database.StateData.InsertOne(new CounterDto
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
                Assert.Equal(1, connection.Database.StateData.OfType<AggregatedCounterDto>().Count(new BsonDocument()));
            }
        }
    }
#pragma warning restore 1591
}