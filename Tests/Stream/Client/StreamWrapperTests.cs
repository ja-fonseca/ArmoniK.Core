﻿// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Stream.Client;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

[TestFixture]
internal class StreamWrapperTests
{
  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string> baseConfig = new()
                                            {
                                              {
                                                "GrpcClient:Endpoint", "http://localhost:5001"
                                              },
                                            };

    var builder = new ConfigurationBuilder().AddInMemoryCollection(baseConfig)
                                            .AddEnvironmentVariables();
    var configuration = builder.Build();
    var options       = configuration.GetRequiredValue<ArmoniK.Core.Common.Options.GrpcClient>(Core.Common.Options.GrpcClient.SettingSection);

    Console.WriteLine($"endpoint : {options.Endpoint}");
    var channel = GrpcChannelFactory.CreateChannel(options);
    client_ = new Submitter.SubmitterClient(channel);
    Console.WriteLine("Client created");
  }

  private Submitter.SubmitterClient client_;

  [TestCase(2,
            ExpectedResult = 4)]
  [TestCase(4,
            ExpectedResult = 16)]
  public async Task<int> Square(int input)
  {
    var sessionId = Guid.NewGuid() + "mytestsession";
    var taskId    = Guid.NewGuid() + "mytask";

    client_.CreateSessionAndCheckReply(sessionId);

    var payload = new TestPayload
                  {
                    Type      = TestPayload.TaskType.Compute,
                    DataBytes = BitConverter.GetBytes(input),
                    ResultKey = taskId,
                  };

    var req = new TaskRequest
              {
                Id      = taskId,
                Payload = ByteString.CopyFrom(payload.Serialize()),
                ExpectedOutputKeys =
                {
                  taskId,
                },
              };

    Console.WriteLine("TaskRequest Created");

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      null,
                                      new[]
                                      {
                                        req,
                                      })
                 .ConfigureAwait(false);

    var resultRequest = new ResultRequest
                        {
                          Key     = taskId,
                          Session = sessionId,
                        };

    var availabilityReply = client_.WaitForAvailability(resultRequest);

    Assert.AreEqual(availabilityReply.TypeCase,
                    AvailabilityReply.TypeOneofCase.Ok);

    var streamingCall = client_.TryGetResultStream(resultRequest);

    var result = new List<byte>();

    var resultPayload = TestPayload.Deserialize(await client_.GetResultAsync(resultRequest)
                                                             .ConfigureAwait(false));
    Console.WriteLine($"Payload Type : {resultPayload.Type} - {taskId}");
    if (resultPayload.Type == TestPayload.TaskType.Result)
    {
      var output = BitConverter.ToInt32(resultPayload.DataBytes);
      Console.WriteLine($"Result : {output}");
      return output;
    }

    return 0;
  }

  [Test(ExpectedResult = Output.TypeOneofCase.Error)]
  [Repeat(2)]
  public async Task<Output.TypeOneofCase> TaskError()
  {
    var sessionId = Guid.NewGuid() + "mytestsession";
    var taskId    = Guid.NewGuid() + "mytask";

    client_.CreateSessionAndCheckReply(sessionId);

    var payload = new TestPayload
                  {
                    Type = TestPayload.TaskType.Error,
                  };

    var req = new TaskRequest
              {
                Id      = taskId,
                Payload = ByteString.CopyFrom(payload.Serialize()),
                ExpectedOutputKeys =
                {
                  taskId,
                },
              };

    Console.WriteLine("TaskRequest Created");

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      null,
                                      new[]
                                      {
                                        req,
                                      })
                 .ConfigureAwait(false);

    var resultRequest = new ResultRequest
                        {
                          Key     = taskId,
                          Session = sessionId,
                        };

    var taskOutput = client_.TryGetTaskOutput(resultRequest);
    Console.WriteLine(taskOutput.ToString());
    return taskOutput.TypeCase;
  }

  // TODO: should it be TaskStatus.Failed ?
  [Test]
  [Repeat(2)]
  public async Task TaskFailed()
  {
    var sessionId = Guid.NewGuid() + nameof(TaskFailed);

    client_.CreateSessionAndCheckReply(sessionId);

    var payload = new TestPayload
                  {
                    Type = TestPayload.TaskType.ReturnFailed,
                  };

    var taskRequests = new List<TaskRequest>();

    for (var i = 0; i < 10; i++)
    {
      var taskId = Guid.NewGuid() + "mytask";
      var req = new TaskRequest
                {
                  Id      = taskId,
                  Payload = ByteString.CopyFrom(payload.Serialize()),
                  ExpectedOutputKeys =
                  {
                    taskId,
                  },
                };
      taskRequests.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      null,
                                      taskRequests)
                 .ConfigureAwait(false);

    var taskOutput = taskRequests.Select(request =>
                                         {
                                           var resultRequest = new ResultRequest
                                                               {
                                                                 Key     = request.Id,
                                                                 Session = sessionId,
                                                               };

                                           var taskOutput = client_.TryGetTaskOutput(resultRequest);
                                           Console.WriteLine(request.Id + " - " + taskOutput);
                                           return taskOutput.Status;
                                         });

    Assert.IsTrue(taskOutput.All(status => status == TaskStatus.Error));
  }


  [Test]
  public async Task MultipleTasks([Values(4,
                                          5,
                                          100)]
                                  int n,
                                  [Values(TestPayload.TaskType.Compute,
                                          TestPayload.TaskType.Transfer)]
                                  TestPayload.TaskType taskType)
  {
    var sessionId = "sessionId-" + Guid.NewGuid() + "-" + nameof(MultipleTasks) + " - " + taskType;
    Console.WriteLine($"Type of task {taskType}");

    client_.CreateSessionAndCheckReply(sessionId);

    var taskRequestList = new List<TaskRequest>();

    for (var i = 0; i < n; i++)
    {
      var taskId = nameof(MultipleTasks) + "-" + i + "-" + Guid.NewGuid();

      var payload = new TestPayload
                    {
                      Type      = taskType,
                      DataBytes = BitConverter.GetBytes(i),
                      ResultKey = taskId,
                    };

      var req = new TaskRequest
                {
                  Id      = taskId,
                  Payload = ByteString.CopyFrom(payload.Serialize()),
                  ExpectedOutputKeys =
                  {
                    taskId,
                  },
                };
      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      null,
                                      taskRequestList)
                 .ConfigureAwait(false);

    var resultAvailability = taskRequestList.Select(request =>
                                                    {
                                                      var resultRequest = new ResultRequest
                                                                          {
                                                                            Key     = request.Id,
                                                                            Session = sessionId,
                                                                          };
                                                      var availabilityReply = client_.WaitForAvailability(resultRequest);
                                                      return availabilityReply.TypeCase;
                                                    });

    Assert.IsTrue(resultAvailability.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultTypeOneofCases = taskRequestList.Select(request =>
                                                      {
                                                        var resultRequest = new ResultRequest
                                                                            {
                                                                              Key     = request.Id,
                                                                              Session = sessionId,
                                                                            };
                                                        var taskOutput = client_.TryGetTaskOutput(resultRequest);
                                                        Console.WriteLine(request.Id + " - " + taskOutput);
                                                        return taskOutput.TypeCase;
                                                      });

    Assert.IsTrue(resultTypeOneofCases.All(t => t == Output.TypeOneofCase.Ok));

    var resultList = taskRequestList.Select(async request =>
                                            {
                                              var resultRequest = new ResultRequest
                                                                  {
                                                                    Key     = request.Id,
                                                                    Session = sessionId,
                                                                  };

                                              var resultPayload = TestPayload.Deserialize(await client_.GetResultAsync(resultRequest)
                                                                                                       .ConfigureAwait(false));
                                              Console.WriteLine($"Payload Type : {resultPayload.Type} - {request.Id}");
                                              if (resultPayload.Type == TestPayload.TaskType.Result)
                                              {
                                                var output = BitConverter.ToInt32(resultPayload.DataBytes);
                                                Console.WriteLine($"Result : {output}");
                                                return output;
                                              }

                                              return 0;
                                            });

    var sum = resultList.Aggregate((t1,
                                    t2) => Task.FromResult(t1.Result + t2.Result));
    Assert.AreEqual(n * (n - 1) * (2 * n - 1) / 6,
                    sum.Result);
  }

  [Test]
  public async Task MultipleDataDependencies([Values(1,
                                                     5,
                                                     20)]
                                             int n)
  {
    var sessionId = Guid.NewGuid() + "-MultipleDatadependencies";

    client_.CreateSessionAndCheckReply(sessionId);

    var taskRequestList = new List<TaskRequest>();

    for (var i = 0; i < n; i++)
    {
      var taskId = "datadep-" + i + "-" + Guid.NewGuid();

      var payload = new TestPayload
                    {
                      Type      = TestPayload.TaskType.DatadepTransfer,
                      DataBytes = BitConverter.GetBytes(i + 5),
                      ResultKey = taskId,
                    };

      var req = new TaskRequest
                {
                  Id      = taskId,
                  Payload = ByteString.CopyFrom(payload.Serialize()),
                  ExpectedOutputKeys =
                  {
                    taskId + "-res1",
                    taskId + "-res2",
                  },
                };
      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      null,
                                      taskRequestList)
                 .ConfigureAwait(false);

    var resultAvailability1 = taskRequestList.Select(request =>
                                                     {
                                                       var resultRequest = new ResultRequest
                                                                           {
                                                                             Key     = request.Id + "-res1",
                                                                             Session = sessionId,
                                                                           };
                                                       var availabilityReply = client_.WaitForAvailability(resultRequest);
                                                       return availabilityReply.TypeCase;
                                                     });

    Assert.IsTrue(resultAvailability1.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultAvailability2 = taskRequestList.Select(request =>
                                                     {
                                                       var resultRequest = new ResultRequest
                                                                           {
                                                                             Key     = request.Id + "-res2",
                                                                             Session = sessionId,
                                                                           };
                                                       var availabilityReply = client_.WaitForAvailability(resultRequest);
                                                       return availabilityReply.TypeCase;
                                                     });

    Assert.IsTrue(resultAvailability2.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultTypeOneofCases = taskRequestList.Select(request =>
                                                      {
                                                        var resultRequest = new ResultRequest
                                                                            {
                                                                              Key     = request.Id,
                                                                              Session = sessionId,
                                                                            };
                                                        var taskOutput = client_.TryGetTaskOutput(resultRequest);
                                                        Console.WriteLine(request.Id + " - " + taskOutput);
                                                        return taskOutput.TypeCase;
                                                      });

    Assert.IsTrue(resultTypeOneofCases.All(t => t == Output.TypeOneofCase.Ok));

    var results = taskRequestList.Select(async request =>
                                         {
                                           var resultRequest1 = new ResultRequest
                                                                {
                                                                  Key     = request.Id + "-res1",
                                                                  Session = sessionId,
                                                                };
                                           var resultBytes1 = await client_.GetResultAsync(resultRequest1)
                                                                           .ConfigureAwait(false);
                                           if (resultBytes1.Length == 0)
                                           {
                                             throw new Exception();
                                           }

                                           var resultPayload1 = TestPayload.Deserialize(resultBytes1);

                                           var resultRequest2 = new ResultRequest
                                                                {
                                                                  Key     = request.Id + "-res2",
                                                                  Session = sessionId,
                                                                };
                                           var resultBytes2 = await client_.GetResultAsync(resultRequest2)
                                                                           .ConfigureAwait(false);
                                           if (resultBytes2.Length == 0)
                                           {
                                             throw new Exception();
                                           }

                                           var resultPayload2 = TestPayload.Deserialize(resultBytes2);

                                           var resultInt1 = BitConverter.ToInt32(resultPayload1.DataBytes);
                                           var resultInt2 = BitConverter.ToInt32(resultPayload2.DataBytes);

                                           Console.WriteLine($"Result1 {resultInt1}");
                                           Console.WriteLine($"Result2 {resultInt2}");

                                           return 2 * resultInt2 == resultInt1;
                                         });
    Assert.IsTrue(results.All(task => task.Result));
  }


  [Test]
  public async Task LargePayloads([Values(2,
                                          10)]
                                  int n,
                                  [Values(1,
                                          2,
                                          5,
                                          10)]
                                  int size)
  {
    var sessionId = Guid.NewGuid() + "-" + nameof(LargePayloads);

    client_.CreateSessionAndCheckReply(sessionId);

    var taskRequestList = new List<TaskRequest>();

    var rnd       = new Random();
    var dataBytes = new byte[size * 1024 * 128];
    rnd.NextBytes(dataBytes);
    var hash = SHA256.HashData(dataBytes);

    var payload = new TestPayload
                  {
                    Type      = TestPayload.TaskType.PayloadCheckSum,
                    DataBytes = dataBytes,
                  };
    var serializedPayload = payload.Serialize();
    var byteString        = ByteString.CopyFrom(serializedPayload);
    Console.WriteLine("Payload Hash " + Convert.ToBase64String(SHA256.HashData(serializedPayload)));
    Console.WriteLine($"Payload size {serializedPayload.Length}");

    for (var i = 0; i < n; i++)
    {
      var taskId = nameof(LargePayloads) + "-" + i + "-" + Guid.NewGuid();
      var req = new TaskRequest
                {
                  Id      = taskId,
                  Payload = byteString,
                  ExpectedOutputKeys =
                  {
                    taskId,
                  },
                };

      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      null,
                                      taskRequestList)
                 .ConfigureAwait(false);

    var resultAvailability = taskRequestList.Select(request =>
                                                    {
                                                      var resultRequest = new ResultRequest
                                                                          {
                                                                            Key     = request.Id,
                                                                            Session = sessionId,
                                                                          };
                                                      var availabilityReply = client_.WaitForAvailability(resultRequest);
                                                      return availabilityReply.TypeCase;
                                                    });

    Assert.IsTrue(resultAvailability.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultTypeOneofCases = taskRequestList.Select(request =>
                                                      {
                                                        var resultRequest = new ResultRequest
                                                                            {
                                                                              Key     = request.Id,
                                                                              Session = sessionId,
                                                                            };
                                                        var taskOutput = client_.TryGetTaskOutput(resultRequest);
                                                        Console.WriteLine(request.Id + " - " + taskOutput);
                                                        return taskOutput.TypeCase;
                                                      });

    Assert.IsTrue(resultTypeOneofCases.All(t => t == Output.TypeOneofCase.Ok));

    var resultList = taskRequestList.Select(async request =>
                                            {
                                              var resultRequest = new ResultRequest
                                                                  {
                                                                    Key     = request.Id,
                                                                    Session = sessionId,
                                                                  };

                                              var resultPayload = TestPayload.Deserialize(await client_.GetResultAsync(resultRequest)
                                                                                                       .ConfigureAwait(false));
                                              Console.WriteLine($"Payload Type : {resultPayload.Type} - {request.Id}");
                                              if (resultPayload.Type == TestPayload.TaskType.Result)
                                              {
                                                return hash.SequenceEqual(resultPayload.DataBytes);
                                              }

                                              return false;
                                            });

    Assert.IsTrue(resultList.All(_ => true));
  }

  [Test]
  public async Task EmptyPayload()
  {
    var sessionId = Guid.NewGuid() + "-" + nameof(LargePayloads);

    client_.CreateSessionAndCheckReply(sessionId);

    var taskId = nameof(LargePayloads) + "-" + Guid.NewGuid();

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      null,
                                      new[]
                                      {
                                        new TaskRequest
                                        {
                                          Id = taskId,
                                          ExpectedOutputKeys =
                                          {
                                            taskId,
                                          },
                                          Payload = ByteString.Empty,
                                        },
                                      })
                 .ConfigureAwait(false);

    var resultRequest = new ResultRequest
                        {
                          Key     = taskId,
                          Session = sessionId,
                        };
    var availabilityReply = client_.WaitForAvailability(resultRequest);

    Assert.IsTrue(availabilityReply.TypeCase == AvailabilityReply.TypeOneofCase.Ok);

    var taskOutput = client_.TryGetTaskOutput(resultRequest);
    Console.WriteLine(taskId + " - " + taskOutput);

    Assert.IsTrue(taskOutput.TypeCase == Output.TypeOneofCase.Error);
  }

  [Test]
  public async Task PriorityShouldHaveAnEffect([Values(10, 50)]
                                  int n)
  {
    var sessionId = Guid.NewGuid() + "-" + nameof(PriorityShouldHaveAnEffect);

    client_.CreateSessionAndCheckReply(sessionId);

    var tasks = Enumerable.Range(1,
                                 9).Select(i => Task.Run(() => RunForPriority(sessionId,
                                                                              i,
                                                                              n))).ToList();

    await tasks.WhenAll()
               .ConfigureAwait(false);
  }

  private async Task<long> RunForPriority(string sessionId, int priority, int n)
  {
    Console.WriteLine("Launch taks with priority " + priority);
    var sw = Stopwatch.StartNew();
    var taskOptions = new TaskOptions
    {
      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
      MaxRetries = 3,
      Priority = priority,
    };
    var taskRequestList = new List<TaskRequest>();

    for (var i = 0; i < n; i++)
    {
      var taskId = nameof(LargePayloads) + "-" + i + "-" + Guid.NewGuid();

      var payload = new TestPayload
      {
        Type = TestPayload.TaskType.Compute,
        DataBytes = BitConverter.GetBytes(1),
        ResultKey = taskId,
      };

      var req = new TaskRequest
      {
        Id = taskId,
        Payload = ByteString.CopyFrom(payload.Serialize()),
        ExpectedOutputKeys =
        {
          taskId,
        },
      };

      taskRequestList.Add(req);
    }

    await client_.CreateTasksAndCheckReplyAsync(sessionId,
                                      taskOptions,
                                      taskRequestList)
                 .ConfigureAwait(false);

    var resultAvailability = taskRequestList.Select(request =>
                                                    {
                                                      var resultRequest = new ResultRequest
                                                                          {
                                                                            Key = request.Id,
                                                                            Session = sessionId,
                                                                          };
                                                      var availabilityReply = client_.WaitForAvailability(resultRequest);
                                                      return availabilityReply.TypeCase;
                                                    });

    if (resultAvailability.Any(c => c != AvailabilityReply.TypeOneofCase.Ok))
      return -1;

    var resultTypeOneofCases = taskRequestList.Select(request =>
    {
      var resultRequest = new ResultRequest
      {
        Key = request.Id,
        Session = sessionId,
      };
      var taskOutput = client_.TryGetTaskOutput(resultRequest);
      return taskOutput.TypeCase;
    });

    if (resultTypeOneofCases.Any(c => c != Output.TypeOneofCase.Ok))
      return -2;

    var resultList = taskRequestList.Select(async request =>
    {
      var resultRequest = new ResultRequest
      {
        Key = request.Id,
        Session = sessionId,
      };

      var resultPayload = TestPayload.Deserialize(await client_.GetResultAsync(resultRequest)
                                                               .ConfigureAwait(false));
      return resultPayload.Type == TestPayload.TaskType.Result;
    });
    Console.WriteLine("Executed taks with priority " + priority + " in " + sw.ElapsedMilliseconds);
    return resultList.All(task => task.Result) ? sw.ElapsedMilliseconds : 0;
  }
}
