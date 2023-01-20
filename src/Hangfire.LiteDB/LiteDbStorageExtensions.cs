using System;
using Hangfire.Annotations;
using LiteDB;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public static class LiteDbStorageExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IGlobalConfiguration<LiteDbStorage> UseLiteDbStorage(
            [NotNull] this IGlobalConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            var storage = new LiteDbStorage("Hangfire.db", new LiteDbStorageOptions());
            return configuration.UseStorage(storage);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="configuration"></param>
        /// <param name="nameOrConnectionString"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IGlobalConfiguration<LiteDbStorage> UseLiteDbStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] string nameOrConnectionString, 
            LiteDbStorageOptions options = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));
            if(options == null) options = new LiteDbStorageOptions();
            var storage = new LiteDbStorage(nameOrConnectionString, options);
            return configuration.UseStorage(storage);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="configuration"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IGlobalConfiguration<LiteDbStorage> UseLiteDbStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] LiteDatabase database)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (database == null) throw new ArgumentNullException(nameof(database));

            var repo = new LiteRepository(database);
            var storage = new LiteDbStorage(repo);
            return configuration.UseStorage(storage);
        }
    }
}