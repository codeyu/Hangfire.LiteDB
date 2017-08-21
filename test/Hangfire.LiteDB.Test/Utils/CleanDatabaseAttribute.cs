using System;
using System.Reflection;
using System.Threading;
using LiteDB;
using Xunit.Sdk;

namespace Hangfire.LiteDB.Test.Utils
{
#pragma warning disable 1591
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);

            RecreateDatabaseAndInstallObjects();
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(GlobalLock);
        }

        private static void RecreateDatabaseAndInstallObjects()
        {
            using (var context = ConnectionUtils.CreateConnection())
            {
                try
                {
                    context.Init(new LiteDbStorageOptions());

                    context.DistributedLock.Delete(new BsonDocument());
                    context.StateDataExpiringKeyValue.Delete(new BsonDocument());
                    context.StateDataHash.Delete(new BsonDocument());
                    context.StateDataSet.Delete(new BsonDocument());
                    context.StateDataList.Delete(new BsonDocument());
                    context.StateDataCounter.Delete(new BsonDocument());
                    context.StateDataAggregatedCounter.Delete(new BsonDocument());
                    context.Job.Delete(new BsonDocument());
                    context.JobQueue.Delete(new BsonDocument());
                    context.Server.Delete(new BsonDocument());

                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Unable to cleanup database.", ex);
                }
            }
        }
    }
#pragma warning restore 1591
}