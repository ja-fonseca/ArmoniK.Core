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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Pollster;

public class PreconditionChecker : IInitializable
{
  private readonly ActivitySource               activitySource_;
  private readonly ILogger<PreconditionChecker> logger_;
  private readonly IResultTable                 resultTable_;
  private readonly ISessionTable                sessionTable_;
  private readonly ITaskTable                   taskTable_;

  private bool isInitialized_;

  public PreconditionChecker(ISessionTable                sessionTable,
                             ITaskTable                   taskTable,
                             IResultTable                 resultTable,
                             ActivitySource               activitySource,
                             ILogger<PreconditionChecker> logger)
  {
    logger_         = logger;
    activitySource_ = activitySource;
    sessionTable_   = sessionTable;
    taskTable_      = taskTable;
    resultTable_    = resultTable;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      var resultTable  = resultTable_.Init(cancellationToken);
      var sessionTable = sessionTable_.Init(cancellationToken);
      var taskTable    = taskTable_.Init(cancellationToken);
      await resultTable.ConfigureAwait(false);
      await sessionTable.ConfigureAwait(false);
      await taskTable.ConfigureAwait(false);
      isInitialized_ = true;
    }
  }

  public async Task<TaskData?> CheckPreconditionsAsync(IQueueMessageHandler messageHandler,
                                                       CancellationToken    cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CheckPreconditionsAsync)}");

    try
    {
      var taskData = await taskTable_.ReadTaskAsync(messageHandler.TaskId,
                                                    cancellationToken)
                                     .ConfigureAwait(false);

      /*
     * Check preconditions:
     *  - Session is not cancelled
     *  - Task is not cancelled
     *  - Task status is OK
     *  - Dependencies have been checked
     *  - Max number of retries has not been reached
     */

      logger_.LogDebug("Handling the task status ({status})",
                       taskData.Status);
      switch (taskData.Status)
      {
        case TaskStatus.Canceling:
          logger_.LogInformation("Task is being cancelled");
          messageHandler.Status = QueueMessageStatus.Cancelled;
          await taskTable_.UpdateTaskStatusAsync(messageHandler.TaskId,
                                                 TaskStatus.Canceled,
                                                 CancellationToken.None)
                          .ConfigureAwait(false);
          return null;
        case TaskStatus.Completed:
          logger_.LogInformation("Task was already completed");
          messageHandler.Status = QueueMessageStatus.Processed;
          return null;
        case TaskStatus.Creating:
          logger_.LogInformation("Task is still creating");
          messageHandler.Status = QueueMessageStatus.Postponed;
          return null;
        case TaskStatus.Submitted:
          break;
        case TaskStatus.Dispatched:
          break;
        case TaskStatus.Error:
          logger_.LogInformation("Task was on error elsewhere ; task should have been resubmitted");
          messageHandler.Status = QueueMessageStatus.Cancelled;
          return null;
        case TaskStatus.Timeout:
          logger_.LogInformation("Task was timeout elsewhere ; taking over here");
          break;
        case TaskStatus.Canceled:
          logger_.LogInformation("Task has been cancelled");
          messageHandler.Status = QueueMessageStatus.Cancelled;
          return null;
        case TaskStatus.Processing:
          logger_.LogInformation("Task is processing elsewhere ; taking over here");
          break;
        case TaskStatus.Failed:
          logger_.LogInformation("Task is failed");
          messageHandler.Status = QueueMessageStatus.Poisonous;
          return null;
        case TaskStatus.Unspecified:
        default:
          logger_.LogCritical("Task was in an unknown state {state}",
                              taskData.Status);
          throw new ArgumentException(nameof(taskData));
      }

      var dependencyCheckTask = taskData.DataDependencies.Any()
                                  ? resultTable_.AreResultsAvailableAsync(taskData.SessionId,
                                                                          taskData.DataDependencies,
                                                                          cancellationToken)
                                  : Task.FromResult(true);

      var isSessionCancelled = await sessionTable_.IsSessionCancelledAsync(taskData.SessionId,
                                                                           cancellationToken)
                                                  .ConfigureAwait(false);

      if (isSessionCancelled && taskData.Status is not (TaskStatus.Canceled or TaskStatus.Completed or TaskStatus.Error))
      {
        logger_.LogInformation("Task is being cancelled");

        messageHandler.Status = QueueMessageStatus.Cancelled;
        await taskTable_.UpdateTaskStatusAsync(messageHandler.TaskId,
                                               TaskStatus.Canceled,
                                               cancellationToken)
                        .ConfigureAwait(false);
        return null;
      }


      if (!await dependencyCheckTask.ConfigureAwait(false))
      {
        logger_.LogDebug("Dependencies are not complete yet.");
        messageHandler.Status = QueueMessageStatus.Postponed;
        return null;
      }


      logger_.LogDebug("checking that the number of retries is not greater than the max retry number");

      var acquireTask = await taskTable_.AcquireTask(messageHandler.TaskId,
                                                     cancellationToken)
                                        .ConfigureAwait(false);
      if (!acquireTask)
      {
        messageHandler.Status = QueueMessageStatus.Postponed;
        return null;
      }

      logger_.LogInformation("Task preconditions are OK");
      return taskData;
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning("TaskId coming from message queue was not found, delete message from queue", e);
      messageHandler.Status = QueueMessageStatus.Processed;
      return null;
    }
  }
}
