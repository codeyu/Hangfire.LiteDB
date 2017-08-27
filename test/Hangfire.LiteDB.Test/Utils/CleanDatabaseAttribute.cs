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
            var context = ConnectionUtils.CreateConnection();
            try
            {
                context.Init(new LiteDbStorageOptions());

                context.DistributedLock.Delete(Query.All());
                context.StateDataExpiringKeyValue.Delete(Query.All());
                context.StateDataHash.Delete(Query.All());
                context.StateDataSet.Delete(Query.All());
                context.StateDataList.Delete(Query.All());
                context.StateDataCounter.Delete(Query.All());
                context.StateDataAggregatedCounter.Delete(Query.All());
                context.Job.Delete(Query.All());
                context.JobQueue.Delete(Query.All());
                context.Server.Delete(Query.All());

            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Unable to cleanup database.", ex);
            }
        }
    }
#pragma warning restore 1591
}