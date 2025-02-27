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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.StateMachines;

using Google.Protobuf;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging.Abstractions;

namespace ArmoniK.Core.Common.Stream.Worker;

[PublicAPI]
public static class TaskRequestExtensions
{
  public static IEnumerable<ProcessReply.Types.CreateLargeTaskRequest> ToRequestStream(this IEnumerable<TaskRequest> taskRequests,
                                                                                       TaskOptions?                  taskOptions,
                                                                                       int                           chunkMaxSize)
  {
    var fsm = new ProcessReplyCreateLargeTaskStateMachine(NullLogger.Instance);
    fsm.InitRequest();
    if (taskOptions is not null)
    {
      yield return new ProcessReply.Types.CreateLargeTaskRequest
                   {
                     InitRequest = new ProcessReply.Types.CreateLargeTaskRequest.Types.InitRequest
                                   {
                                     TaskOptions = taskOptions,
                                   },
                   };
    }
    else
    {
      yield return new ProcessReply.Types.CreateLargeTaskRequest
                   {
                     InitRequest = new ProcessReply.Types.CreateLargeTaskRequest.Types.InitRequest(),
                   };
    }

    using var taskRequestEnumerator = taskRequests.GetEnumerator();

    if (!taskRequestEnumerator.MoveNext())
    {
      yield break;
    }

    var currentRequest = taskRequestEnumerator.Current;

    while (taskRequestEnumerator.MoveNext())
    {
      foreach (var createLargeTaskRequest in currentRequest.ToRequestStream(false,
                                                                            chunkMaxSize,
                                                                            fsm))
      {
        yield return createLargeTaskRequest;
      }


      currentRequest = taskRequestEnumerator.Current;
    }

    foreach (var createLargeTaskRequest in currentRequest.ToRequestStream(true,
                                                                          chunkMaxSize,
                                                                          fsm))
    {
      yield return createLargeTaskRequest;
    }

    if (!fsm.IsComplete())
    {
      throw new ArmoniKException("Create task request should be complete at this point");
    }
  }

  public static IEnumerable<ProcessReply.Types.CreateLargeTaskRequest> ToRequestStream(this TaskRequest                        taskRequest,
                                                                                       bool                                    isLast,
                                                                                       int                                     chunkMaxSize,
                                                                                       ProcessReplyCreateLargeTaskStateMachine processReplyCreateLargeTaskStateMachine)
  {
    processReplyCreateLargeTaskStateMachine.AddHeader();
    yield return new ProcessReply.Types.CreateLargeTaskRequest
                 {
                   InitTask = new InitTaskRequest
                              {
                                Header = new TaskRequestHeader
                                         {
                                           DataDependencies =
                                           {
                                             taskRequest.DataDependencies,
                                           },
                                           ExpectedOutputKeys =
                                           {
                                             taskRequest.ExpectedOutputKeys,
                                           },
                                           Id = taskRequest.Id,
                                         },
                              },
                 };

    var start = 0;

    while (start < taskRequest.Payload.Length)
    {
      var chunkSize = Math.Min(chunkMaxSize,
                               taskRequest.Payload.Length - start);

      processReplyCreateLargeTaskStateMachine.AddDataChunk();
      yield return new ProcessReply.Types.CreateLargeTaskRequest
                   {
                     TaskPayload = new DataChunk
                                   {
                                     Data = ByteString.CopyFrom(taskRequest.Payload.Span.Slice(start,
                                                                                               chunkSize)),
                                   },
                   };

      start += chunkSize;
    }

    processReplyCreateLargeTaskStateMachine.CompleteData();
    yield return new ProcessReply.Types.CreateLargeTaskRequest
                 {
                   TaskPayload = new DataChunk
                                 {
                                   DataComplete = true,
                                 },
                 };

    if (isLast)
    {
      processReplyCreateLargeTaskStateMachine.CompleteRequest();
      yield return new ProcessReply.Types.CreateLargeTaskRequest
                   {
                     InitTask = new InitTaskRequest
                                {
                                  LastTask = true,
                                },
                   };
    }
  }
}
