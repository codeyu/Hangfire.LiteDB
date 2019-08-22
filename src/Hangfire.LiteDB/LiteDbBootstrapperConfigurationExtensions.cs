
namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public static class LiteDbBootstrapperConfigurationExtensions
    {
       
        /// <summary>
        /// Configure Hangfire to use LiteDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for LiteDB database, for example 'LiteDB://username:password@host:port'</param>
      
        /// <returns></returns>
        public static LiteDbStorage LiteDbStorage(this IGlobalConfiguration configuration,
            string connectionString)
        {
            return LiteDbStorage(configuration, connectionString, new LiteDbStorageOptions());
        }

        /// <summary>
        /// Configure Hangfire to use LiteDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for LiteDB database, for example 'LiteDB://username:password@host:port'</param>
        
        /// <param name="storageOptions">Storage options</param>
        /// <returns></returns>
        public static LiteDbStorage LiteDbStorage(this IGlobalConfiguration configuration,
            string connectionString,
            LiteDbStorageOptions storageOptions)
        {
            var storage = new LiteDbStorage(connectionString, storageOptions);

            configuration.UseStorage(storage);

            return storage;
        }    
    }
}