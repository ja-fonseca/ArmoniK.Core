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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Adapters.Memory;

public class LockedQueueStorage : ILockedQueueStorage
{
  private static readonly MessageHandler DefaultMessage = new();

  private static readonly KeyValuePair<MessageHandler, MessageHandler> DefaultPair = new(DefaultMessage,
                                                                                         DefaultMessage);

  private readonly ConcurrentDictionary<string, MessageHandler> id2Handlers_ = new();

  private readonly SortedList<MessageHandler, MessageHandler> queues_ = new(MessageComparer.Instance);

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(true);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public int MaxPriority
    => 100;

  /// <inheritdoc />
  public async IAsyncEnumerable<IQueueMessageHandler> PullAsync(int                                        nbMessages,
                                                                [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    while (nbMessages > 0 && queues_.Any())
    {
      cancellationToken.ThrowIfCancellationRequested();
      var (message, _) = queues_.FirstOrDefault(pair => pair.Key.IsVisible = true,
                                                DefaultPair);
      if (ReferenceEquals(message,
                          DefaultMessage))
      {
        continue;
      }

      try
      {
        await message.Semaphore.WaitAsync(CancellationToken.None)
                     .ConfigureAwait(false);
        if (message.IsVisible)
        {
          message.IsVisible = false;
          yield return message;
        }
        else
        {
          continue;
        }
      }
      finally
      {
        message.Semaphore.Release();
      }

      nbMessages--;
    }
  }

  /// <inheritdoc />
  public Task EnqueueMessagesAsync(IEnumerable<string> messages,
                                   int                 priority          = 1,
                                   CancellationToken   cancellationToken = default)
  {
    var messageHandlers = messages.Select(message => new MessageHandler
                                                     {
                                                       IsVisible         = true,
                                                       Priority          = priority,
                                                       TaskId            = message,
                                                       CancellationToken = CancellationToken.None,
                                                       Status            = QueueMessageStatus.Waiting,
                                                     });
    foreach (var messageHandler in messageHandlers)
    {
      queues_.Add(messageHandler,
                  messageHandler);
      if (!id2Handlers_.TryAdd(messageHandler.TaskId,
                               messageHandler))
      {
        throw new InvalidOperationException("Duplicate messageId found.");
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public TimeSpan LockRefreshPeriodicity { get; } = TimeSpan.FromMinutes(5);

  /// <inheritdoc />
  public TimeSpan LockRefreshExtension { get; } = TimeSpan.FromMinutes(10);

  /// <inheritdoc />
  public bool AreMessagesUnique
    => true;

  /// <inheritdoc />
  public Task<bool> RenewDeadlineAsync(string            id,
                                       CancellationToken cancellationToken = default)
    => Task.FromResult(true);

  /// <inheritdoc />
  public Task MessageProcessedAsync(string            id,
                                    CancellationToken cancellationToken = default)
  {
    if (!id2Handlers_.TryRemove(id,
                                out var handler))
    {
      throw new KeyNotFoundException();
    }

    if (handler.IsVisible)
    {
      throw new InvalidOperationException("Cannot change the status of a message that is visible.");
    }

    if (!queues_.Remove(handler,
                        out _))
    {
      throw new KeyNotFoundException();
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task MessageRejectedAsync(string            id,
                                   CancellationToken cancellationToken = default)
  {
    if (!id2Handlers_.TryRemove(id,
                                out var handler))
    {
      throw new KeyNotFoundException();
    }

    if (handler.IsVisible)
    {
      throw new InvalidOperationException("Cannot change the status of a message that is visible.");
    }

    if (!queues_.Remove(handler,
                        out _))
    {
      throw new KeyNotFoundException();
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task RequeueMessageAsync(string            id,
                                  CancellationToken cancellationToken = default)
  {
    if (!id2Handlers_.TryRemove(id,
                                out var handler))
    {
      throw new KeyNotFoundException();
    }

    if (handler.IsVisible)
    {
      throw new InvalidOperationException("Cannot change the status of a message that is visible.");
    }

    var newMessage = new MessageHandler
                     {
                       IsVisible         = true,
                       Priority          = handler.Priority,
                       TaskId            = handler.TaskId,
                       CancellationToken = CancellationToken.None,
                       Status            = QueueMessageStatus.Waiting,
                     };

    queues_.Add(newMessage,
                newMessage);
    if (!id2Handlers_.TryAdd(newMessage.TaskId,
                             newMessage))
    {
      throw new InvalidOperationException("Duplicate messageId found.");
    }

    if (!queues_.Remove(handler,
                        out _))
    {
      throw new KeyNotFoundException();
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task ReleaseMessageAsync(string            id,
                                  CancellationToken cancellationToken = default)
  {
    if (!id2Handlers_.TryRemove(id,
                                out var handler))
    {
      throw new KeyNotFoundException();
    }

    handler.IsVisible = true;
    return Task.CompletedTask;
  }

  private class MessageHandler : IQueueMessageHandler
  {
    private static long count_;

    public int Priority { get; init; }

    public bool IsVisible { get; set; }

    public SemaphoreSlim Semaphore { get; } = new(1);

    public long Order { get; } = Interlocked.Increment(ref count_);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
      Semaphore.Dispose();
      return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public CancellationToken CancellationToken { get; init; }

    /// <inheritdoc />
    public string MessageId
      => $"Message#{Order}";

    /// <inheritdoc />
    public string TaskId { get; init; }

    /// <inheritdoc />
    public QueueMessageStatus Status { get; set; }
  }

  private class MessageComparer : IComparer<MessageHandler>
  {
    public static readonly IComparer<MessageHandler> Instance = new MessageComparer();

    public int Compare(MessageHandler x,
                       MessageHandler y)
    {
      if (ReferenceEquals(x,
                          y))
      {
        return 0;
      }

      if (y is null)
      {
        return 1;
      }

      if (x is null)
      {
        return -1;
      }

      var priorityComparison = x.Priority.CompareTo(y.Priority);
      return priorityComparison == 0
               ? x.Order.CompareTo(y.Order)
               : priorityComparison;
    }
  }
}
