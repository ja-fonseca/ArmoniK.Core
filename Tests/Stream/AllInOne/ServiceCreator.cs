// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Control.Submitter;
using ArmoniK.Core.Control.Submitter.Services;
using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Mongo2Go;

using MongoDB.Driver;
using Serilog;
using Serilog.Formatting.Compact;

using ILogger = Microsoft.Extensions.Logging.ILogger;
using Worker = ArmoniK.Core.Common.Pollster.Worker;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.AllInOne;

internal class ServiceCreator: IDisposable
{
  private static readonly string                SocketPathCompute = "/cache/armonikcompute.sock";
  private static readonly string                SocketPathSubmitter = "/cache/armoniksubmitter.sock";
  private readonly        WebApplicationBuilder builder_;
  public readonly         ILoggerFactory        LoggerFactory;
  private readonly        ILogger               logger_;
  private readonly        MongoDbRunner         runner_;

  public ServiceCreator()
  {
    Dictionary<string, string> baseConfig = new()
    {
      { "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage" },
      { "Components:QueueStorage", "ArmoniK.Adapters.MongoDB.LockedQueueStorage" },
      { "Components:ObjectStorage", "ArmoniK.Adapters.MongoDB.ObjectStorage" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.Host)}", "localhost" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.Port)}", "3232" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.Tls)}", "true" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.User)}", "user" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.Password)}", "password" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.CredentialsPath)}", "" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.CAFile)}", "" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.ReplicaSet)}", "rs0" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.DatabaseName)}", "database" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.DataRetention)}", "10.00:00:00" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.TableStorage)}:PollingDelay", "00:00:10" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.ObjectStorage)}:ChunkSize", "100000" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.QueueStorage)}:LockRefreshPeriodicity", "00:20:00" },
      { $"{Core.Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Core.Adapters.MongoDB.Options.MongoDB.QueueStorage)}:PollPeriodicity", "00:00:50" },
      { $"{Core.Common.Injection.Options.ComputePlan.SettingSection}:{nameof(Core.Common.Injection.Options.ComputePlan.GrpcChannel)}:Address", SocketPathCompute },
      { $"{Core.Common.Injection.Options.ComputePlan.SettingSection}:{nameof(Core.Common.Injection.Options.ComputePlan.GrpcChannel)}:SocketType", "unixsocket" },
      { $"{Core.Common.Injection.Options.ComputePlan.SettingSection}:{nameof(Core.Common.Injection.Options.ComputePlan.MessageBatchSize)}", "1" },
    };

    builder_ = WebApplication.CreateBuilder();

    builder_.Configuration
           .SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("appsettings.json",
                        true,
                        true)
           .AddInMemoryCollection(baseConfig);

    Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(builder_.Configuration)
                                          .WriteTo.Console(new CompactJsonFormatter())
                                          .WriteTo.File("./logs.txt")
                                          .MinimumLevel.Verbose()
                                          .Enrich.FromLogContext()
                                          .CreateLogger();

    LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(loggingBuilder => loggingBuilder.AddSerilog(Log.Logger));
    logger_       = LoggerFactory.CreateLogger("root");

    builder_.Host
           .UseSerilog(Log.Logger);

    runner_ = MongoDbRunner.Start();

    builder_.Services
           .AddLogging()
           .AddSingleton<IMongoClient>(_ => new MongoClient(runner_.ConnectionString));
  }

  public Submitter.SubmitterClient createSubmitterClient()
  {
    var udsEndPoint = new UnixDomainSocketEndPoint(SocketPathSubmitter);

    var socketsHttpHandler = new SocketsHttpHandler
    {
      ConnectCallback = async (unknown, cancellationToken) =>
      {
        var socket = new Socket(AddressFamily.Unix,
                                SocketType.Stream,
                                ProtocolType.Unspecified);

        try
        {
          await socket.ConnectAsync(udsEndPoint,
                                    cancellationToken).ConfigureAwait(false);
          return new NetworkStream(socket,
                                   true);
        }
        catch
        {
          socket.Dispose();
          throw;
        }
      },
    };

    var channel = Grpc.Net.Client.GrpcChannel.ForAddress("http://localhost",
                                                  new()
                                                  {
                                                    HttpHandler = socketsHttpHandler,
                                                  });
    return new Submitter.SubmitterClient(channel);
  }

  public Task CreateSubmitter()
  {
    builder_.WebHost.ConfigureKestrel(options =>
    {
      if (File.Exists(SocketPathSubmitter))
      {
        File.Delete(SocketPathSubmitter);
      }

      options.ListenUnixSocket(SocketPathSubmitter,
                               listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
    });

    builder_.Services
           .AddArmoniKCore(builder_.Configuration)
           .AddMongoStorages(builder_.Configuration,
                             logger_)
           .ValidateGrpcRequests();


    var app = builder_.Build();

    if (app.Environment.IsDevelopment())
      app.UseDeveloperExceptionPage();

    app.UseRouting();

    app.UseEndpoints(endpoints =>
    {
      endpoints.MapHealthChecks("/startup",
                                new()
                                {
                                  Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Startup)),
                                });

      endpoints.MapHealthChecks("/liveness",
                                new()
                                {
                                  Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Liveness)),
                                });

      //readiness uses grpc to ensure corresponding features are ok.
      endpoints.MapGrpcService<GrpcHealthCheckService>();

      endpoints.MapGrpcService<GrpcSubmitterService>();

      if (app.Environment.IsDevelopment())
        endpoints.MapGrpcReflectionService();
    });
    return app.RunAsync();
  }

  public Task CreatePollster()
  {
    builder_.Services
           .AddArmoniKCore(builder_.Configuration)
           .AddMongoStorages(builder_.Configuration,
                             logger_)
           .AddHostedService<Worker>()
           .AddSingleton<Pollster>()
           .AddSingleton<PreconditionChecker>()
           .AddSingleton<RequestProcessor>()
           .AddSingleton<Core.Common.gRPC.Services.Submitter>()
           .AddSingleton<DataPrefetcher>();

    builder_.Services.AddHealthChecks();

    builder_.WebHost
           .UseConfiguration(builder_.Configuration)
           .UseKestrel(options =>
           {
             options.Listen(IPAddress.Loopback,
                            8989,
                            listenOptions => listenOptions.Protocols = HttpProtocols.Http1);
           });

    var app = builder_.Build();

    app.UseRouting();

    if (app.Environment.IsDevelopment())
      app.UseDeveloperExceptionPage();

    app.UseEndpoints(endpoints =>
    {
      endpoints.MapHealthChecks("/startup",
                                new()
                                {
                                  Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Startup)),
                                });

      endpoints.MapHealthChecks("/liveness",
                                new()
                                {
                                  Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Liveness)),
                                });

      endpoints.MapHealthChecks("/readiness",
                                new()
                                {
                                  Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Readiness)),
                                });
    });
    return app.RunAsync();
  }

  public Task CreateWorker()
  {
    builder_.WebHost.ConfigureKestrel(options =>
    {
      if (File.Exists(SocketPathCompute))
      {
        File.Delete(SocketPathCompute);
      }

      options.ListenUnixSocket(SocketPathCompute,
                               listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
    });

    builder_.Services
           .AddSingleton(_ => LoggerFactory)
           .AddGrpc(options => options.MaxReceiveMessageSize = null);


    var app = builder_.Build();

    if (app.Environment.IsDevelopment())
      app.UseDeveloperExceptionPage();

    app.UseRouting();


    app.UseEndpoints(endpoints =>
    {
      endpoints.MapGrpcService<WorkerService>();

      if (app.Environment.IsDevelopment())
      {
        endpoints.MapGrpcReflectionService();
        logger_.LogInformation("Grpc Reflection Activated");
      }
    });

    return app.RunAsync();
  }

  public void Dispose()
  {
    runner_.Dispose();
  }
}