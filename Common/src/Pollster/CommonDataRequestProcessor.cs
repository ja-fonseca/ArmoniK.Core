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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
/// Processor for <see cref="ProcessReply.TypeOneofCase.CommonData"/>
/// </summary>
public class CommonDataRequestProcessor : IProcessReplyProcessor
{
  private readonly IObjectStorage                           resourcesStorage_;
  private readonly IAsyncPipe<ProcessReply, ProcessRequest> pipe_;
  private readonly ILogger                                  logger_;
  private          bool                                     isComplete_;

  /// <summary>
  /// Initializes the class with its required objects
  /// </summary>
  /// <param name="resourcesStorage">Interface class to interact with object storage</param>
  /// <param name="pipe">Interface class to exchange requests</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public CommonDataRequestProcessor(IObjectStorage                           resourcesStorage,
                                    IAsyncPipe<ProcessReply, ProcessRequest> pipe,
                                    ILogger                                  logger)
  {
    resourcesStorage_ = resourcesStorage;
    pipe_             = pipe;
    logger_           = logger;
    isComplete_       = false;
  }

  /// <inheritdoc />
  public async Task AddProcessReply(ProcessReply      processReply,
                                    CancellationToken cancellationToken)
  {
    await pipe_.WriteAsync(new ProcessRequest
                           {
                             CommonData = new ProcessRequest.Types.DataReply
                                          {
                                            ReplyId = processReply.RequestId,
                                            Init = new ProcessRequest.Types.DataReply.Types.Init
                                                   {
                                                     Key   = processReply.CommonData.Key,
                                                     Error = "Common data are not supported yet",
                                                   },
                                          },
                           })
               .ConfigureAwait(false);
    isComplete_ = true;
  }

  /// <inheritdoc />
  public bool IsComplete()
    => isComplete_;

  /// <inheritdoc />
  public Task WaitForResponseCompletion(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public Task Cancel()
    => throw new NotImplementedException();

  /// <inheritdoc />
  public Task CompleteProcessing(CancellationToken cancellationToken)
    => Task.CompletedTask;
}
