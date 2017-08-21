using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Hangfire.LiteDB.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDBStorageOptionsFacts
    {
        [Fact]
        public void Ctor_SetsTheDefaultOptions()
        {
            LiteDbStorageOptions storageOptions = new LiteDbStorageOptions();

            Assert.Equal("hangfire", storageOptions.Prefix);
            Assert.True(storageOptions.InvisibilityTimeout > TimeSpan.Zero);
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateClientId()
        {
            var storageOptions = new LiteDbStorageOptions();
            Assert.False(String.IsNullOrWhiteSpace(storageOptions.ClientId));
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateUniqueClientId()
        {
            var storageOptions1 = new LiteDbStorageOptions();
            var storageOptions2 = new LiteDbStorageOptions();
            var storageOptions3 = new LiteDbStorageOptions();

            IEnumerable<string> result = new[] { storageOptions1.ClientId, storageOptions2.ClientId, storageOptions3.ClientId }.Distinct();

            Assert.Equal(3, result.Count());
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
        {
            var storageOptions = new LiteDbStorageOptions();
            Assert.Throws<ArgumentException>(
                () => storageOptions.QueuePollInterval = TimeSpan.Zero);
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsNegative()
        {
            var storageOptions = new LiteDbStorageOptions();
            Assert.Throws<ArgumentException>(
                () => storageOptions.QueuePollInterval = TimeSpan.FromSeconds(-1));
        }

        [Fact]
        public void Set_QueuePollInterval_SetsTheValue()
        {
            var storageOptions = new LiteDbStorageOptions
            {
                QueuePollInterval = TimeSpan.FromSeconds(1)
            };
            Assert.Equal(TimeSpan.FromSeconds(1), storageOptions.QueuePollInterval);
        }
    }
#pragma warning restore 1591
}