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

                context.DistributedLock.DeleteMany(x=>x.Id==x.Id);
                context.StateDataExpiringKeyValue.DeleteMany(x => x.Id == x.Id);
                context.StateDataHash.DeleteMany(x => x.Id == x.Id);
                context.StateDataSet.DeleteMany(x => x.Id == x.Id);
                context.StateDataList.DeleteMany(x => x.Id == x.Id);
                context.StateDataCounter.DeleteMany(x => x.Id == x.Id);
                context.StateDataAggregatedCounter.DeleteMany(x => x.Id == x.Id);
                context.Job.DeleteMany(x => x.Id == x.Id);
                context.JobQueue.DeleteMany(x => x.Id == x.Id);
                context.Server.DeleteMany(x => x.Id == x.Id);

            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Unable to cleanup database.", ex);
            }
        }
    }
#pragma warning restore 1591
}