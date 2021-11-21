﻿// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public static class QueueStorageExt
  {
    public static async Task<string> EnqueueAsync(this IQueueStorage queueStorage, QueueMessage message, CancellationToken cancellationToken = default)
    {
      return await queueStorage.EnqueueMessagesAsync(new[] { message }, cancellationToken).SingleAsync(cancellationToken);
    }

    public static QueueMessageDeadlineHandler GetDeadlineHandler(this IQueueStorage queueStorage,
                                                                 string             messageId,
                                                                 CancellationToken  cancellationToken = default)
      => new (queueStorage, messageId, cancellationToken);
  }
}
