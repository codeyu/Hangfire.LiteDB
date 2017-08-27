using System;
using Hangfire;
using Hangfire.LiteDB;
namespace ConsoleSample
{
    public static class Program
    {
        public static int X;

        public static void Main()
        {
            try {
                // you can use LiteDB Storage and specify the connection string name
                GlobalConfiguration.Configuration
                    .UseColouredConsoleLogProvider()
                    .LiteDbStorage("Hangfire.db");

                //you have to create an instance of background job server at least once for background jobs to run
                using (new BackgroundJobServer())
                {
                    // Run once
                    BackgroundJob.Enqueue(() => Console.WriteLine("Background Job: Hello, world!"));

                    BackgroundJob.Enqueue(() => Test());

                    // Run every minute
                    RecurringJob.AddOrUpdate(() => Test(), Cron.Minutely);

                    Console.WriteLine("Press Enter to exit...");
                    Console.ReadLine();
                }

                
            } catch (Exception) {
                throw;
            }
        }

        [AutomaticRetry(Attempts = 2, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        public static void Test()
        {
            Console.WriteLine($"{X++} Cron Job: Hello, world!");
            //throw new ArgumentException("fail");
        }
    }
}