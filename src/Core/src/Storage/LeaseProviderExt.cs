﻿// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

namespace ArmoniK.Core.Storage
{
  public static class LeaseProviderExt
  {
    public static async Task<LeaseHandler> GetLeaseHandler(this ILeaseProvider leaseProvider,
                                                           TaskId              taskId,
                                                           CancellationToken   cancellationToken = default)
    {
      var output = new LeaseHandler(leaseProvider, taskId, cancellationToken);
      await output.Start();
      return output;
    }
  }
}
