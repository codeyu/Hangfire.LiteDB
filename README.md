# Hangfire.LiteDB
[![NuGet Badge](https://buildstats.info/nuget/Hangfire.LiteDB)](https://www.nuget.org/packages/Hangfire.LiteDB/) [![MyGet Pre Release](https://img.shields.io/myget/hangfire-litedb/vpre/Hangfire.LiteDB.svg)](https://www.myget.org/feed/hangfire-litedb/package/nuget/Hangfire.LiteDB)
## Build Status
`Platform` | `Master`
--- | ---
**Windows** | [![Build status](https://ci.appveyor.com/api/projects/status/yre8t19rdaxax7e6?svg=true)](https://ci.appveyor.com/project/codeyu/hangfire-litedb)
**Linux / OS X** | [![Build Status](https://travis-ci.org/codeyu/Hangfire.LiteDB.svg?branch=master)](https://travis-ci.org/codeyu/Hangfire.LiteDB)

## Overview

LiteDB job storage for Hangfire

## Usage

This is how you connect to an litedb instance
```csharp
GlobalConfiguration.Configuration.UseLiteDbStorage();
```

To enqueue a background job you must have the following in the code somewhere at least once or the background job queue will not process
```csharp
var client = new BackgroundJobServer();
\\then you can do this, which runs once
BackgroundJob.Enqueue(() => Console.WriteLine("Background Job: Hello, world!"));
```

[**Delayed tasks**](http://docs.hangfire.io/en/latest/users-guide/background-methods/calling-methods-with-delay.html)

Scheduled background jobs are being executed only after given amount of time.

```csharp
BackgroundJob.Schedule(() => Console.WriteLine("Reliable!"), TimeSpan.FromDays(7));
```

[**Recurring tasks**](http://docs.hangfire.io/en/latest/users-guide/background-methods/performing-recurrent-tasks.html)

Recurring jobs were never been simpler, just call the following method to perform any kind of recurring task using the [CRON expressions](http://en.wikipedia.org/wiki/Cron#CRON_expression).

```csharp
RecurringJob.AddOrUpdate(() => Console.WriteLine("Transparent!"), Cron.Daily);
```

## Continuations

Continuations allow you to define complex workflows by chaining multiple background jobs together.

```csharp
var id = BackgroundJob.Enqueue(() => Console.WriteLine("Hello, "));
BackgroundJob.ContinueWith(id, () => Console.WriteLine("world!"));
```

## License

Hangfire.LiteDB is released under the MIT License.

## Known Bugs

* ~~UTC Time Zone and Local Time Zone is  confusing.~~
