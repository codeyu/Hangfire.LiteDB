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

                context.DistributedLock.DeleteAll();
                context.StateDataExpiringKeyValue.DeleteAll();
                context.StateDataHash.DeleteAll();
                context.StateDataSet.DeleteAll();
                context.StateDataList.DeleteAll();
                context.StateDataCounter.DeleteAll();
                context.StateDataAggregatedCounter.DeleteAll();
                context.Job.DeleteAll();
                context.JobQueue.DeleteAll();
                context.Server.DeleteAll();

            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Unable to cleanup database.", ex);
            }
        }
    }
#pragma warning restore 1591
}