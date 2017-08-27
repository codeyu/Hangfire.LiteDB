using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.LiteDB.Entities;
using Hangfire.Logging;
using Hangfire.Storage;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// Represents distibuted lock implementation for LiteDB
    /// </summary>
    public sealed class LiteDbDistributedLock : IDisposable
    {

        private static readonly ILog Logger = LogProvider.For<LiteDbDistributedLock>();

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
                    = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());


        private readonly string _resource;

        private readonly HangfireDbContext _database;

        private readonly LiteDbStorageOptions _storageOptions;


        private Timer _heartbeatTimer;

        private bool _completed;

        private readonly object _lockObject = new object();

        private string EventWaitHandleName => $@"{GetType().FullName}.{_resource}";


        /// <summary>
        /// Creates LiteDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="database">Lock database</param>
        /// <param name="storageOptions">Database options</param>
        /// <exception cref="DistributedLockTimeoutException">Thrown if lock is not acuired within the timeout</exception>
        /// <exception cref="LiteDbDistributedLockException">Thrown if other LiteDB specific issue prevented the lock to be acquired</exception>
        public LiteDbDistributedLock(string resource, TimeSpan timeout, HangfireDbContext database, LiteDbStorageOptions storageOptions)
        {
            _resource = resource ?? throw new ArgumentNullException(nameof(resource));
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            if (string.IsNullOrEmpty(resource))
            {
                throw new ArgumentException($@"The {nameof(resource)} cannot be empty", nameof(resource));
            }
            if (timeout.TotalSeconds > int.MaxValue)
            {
                throw new ArgumentException($"The timeout specified is too large. Please supply a timeout equal to or less than {int.MaxValue} seconds", nameof(timeout));
            }

            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0)
            {
                Cleanup();
                Acquire(timeout);
                AcquiredLocks.Value[_resource] = 1;
                StartHeartBeat();
            }
            else
            {
                AcquiredLocks.Value[_resource]++;
            }
        }


        /// <summary>
        /// Disposes the object
        /// </summary>
        /// <exception cref="LiteDbDistributedLockException"></exception>
        public void Dispose()
        {
            if (_completed)
            {
                return;
            }
            _completed = true;

            if (!AcquiredLocks.Value.ContainsKey(_resource))
            {
                return;
            }

            AcquiredLocks.Value[_resource]--;

            if (AcquiredLocks.Value[_resource] > 0)
            {
                return;
            }

            // Timer callback may be invoked after the Dispose method call,
            // so we are using lock to avoid unsynchronized calls.
            lock (_lockObject)
            {
                AcquiredLocks.Value.Remove(_resource);

                if (_heartbeatTimer != null)
                {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }

                Release();

                Cleanup();
            }
        }


        private void Acquire(TimeSpan timeout)
        {
            try
            {
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.Now;
                var lockTimeoutTime = now.Add(timeout);

                while ((lockTimeoutTime >= now))
                {
                    var result = _database.DistributedLock.FindOne(x => x.Resource == _resource);
                    var distributedLock = result??new DistributedLock();
                    distributedLock.Resource = _resource;
                    distributedLock.ExpireAt = DateTime.Now.Add(_storageOptions.DistributedLockLifetime);

                    _database.DistributedLock.Upsert(distributedLock);
                    // If result is null, then it means we acquired the lock
                    if (result == null)
                    {
                        isLockAcquired = true;
                    }
                    else
                    {
                        try
                        {
                            // Wait on the event. This allows us to be "woken" up sooner rather than later.
                            // We wait in chunks as we need to "wake-up" from time to time and poll LiteDB,
                            // in case that the lock was acquired on another machine or instance.
                            var eventWaitHandle = new EventWaitHandle(false, EventResetMode.AutoReset, EventWaitHandleName);
                            eventWaitHandle.WaitOne((int)timeout.TotalMilliseconds / 10);
                        }
                        catch (PlatformNotSupportedException)
                        {
                            // EventWaitHandle is not supported on UNIX systems
                            // https://github.com/dotnet/coreclr/pull/1387
                            // Instead of using a compiler directive, we catch the
                            // exception and handles it. This way, when EventWaitHandle
                            // becomes available on UNIX, we will start working.
                            // So in this case, we just sleep for a while and then 
                            // check if the lock has been released.
                            Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                        }
                        now = DateTime.Now;
                    }
                }

                if (!isLockAcquired)
                {
                    throw new DistributedLockTimeoutException($"Could not place a lock on the resource \'{_resource}\': The lock request timed out.");
                }
            }
            catch (DistributedLockTimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new LiteDbDistributedLockException($"Could not place a lock on the resource \'{_resource}\': Check inner exception for details.", ex);
            }
        }


        /// <summary>
        /// Release the lock
        /// </summary>
        /// <exception cref="LiteDbDistributedLockException"></exception>
        private void Release()
        {
            try
            {
                // Remove resource lock
                _database.DistributedLock.Delete(_ => _.Resource== _resource);
                EventWaitHandle eventWaitHandler;
                if (EventWaitHandle.TryOpenExisting(EventWaitHandleName, out eventWaitHandler))
                {
                    eventWaitHandler.Set();
                }
            }
            catch (PlatformNotSupportedException)
            {
                // EventWaitHandle is not supported on UNIX systems
                // https://github.com/dotnet/coreclr/pull/1387
                // So we are not able to signal anyone waiting
                // for this lock, in order for them to acquire
                // it sooner rather than later.
            }
            catch (Exception ex)
            {
                throw new LiteDbDistributedLockException($"Could not release a lock on the resource \'{_resource}\': Check inner exception for details.", ex);
            }
        }


        private void Cleanup()
        {
            try
            {
                // Delete expired locks
                _database.DistributedLock.Delete(x => x.Resource == _resource && x.ExpireAt < DateTime.Now);
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Unable to clean up locks on the resource '{0}'. {1}", _resource, ex);
            }
        }

        /// <summary>
        /// Starts database heartbeat
        /// </summary>
        private void StartHeartBeat()
        {
            TimeSpan timerInterval = TimeSpan.FromMilliseconds(_storageOptions.DistributedLockLifetime.TotalMilliseconds / 5);

            _heartbeatTimer = new Timer(state =>
            {
                // Timer callback may be invoked after the Dispose method call,
                // so we are using lock to avoid unsynchronized calls.
                lock (_lockObject)
                {
                    try
                    {
                        var distributedLock = _database.DistributedLock.FindOne(x => x.Resource == _resource);
                        distributedLock.ExpireAt = DateTime.Now.Add(_storageOptions.DistributedLockLifetime);

                        _database.DistributedLock.Update(distributedLock);
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("Unable to update heartbeat on the resource '{0}'. {1}", _resource, ex);
                    }
                }
            }, null, timerInterval, timerInterval);
        }

    }
}