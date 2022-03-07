﻿// This file is part of the ArmoniK project
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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Object;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.MongoDB;

public class ObjectStorageFactory : IObjectStorageFactory
{
  public ObjectStorageFactory(SessionProvider                                                         sessionProvider,
                              MongoCollectionProvider<ObjectDataModelMapping, ObjectDataModelMapping> objectCollectionProvider,
                              Options.ObjectStorage options,
                              ILoggerFactory loggerFactory)
  {
    sessionProvider_          = sessionProvider;
    objectCollectionProvider_ = objectCollectionProvider;
    options_                  = options;
    loggerFactory_            = loggerFactory;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await objectCollectionProvider_.GetAsync();
    }
    isInitialized_ = true;
  }


  private          bool                                                                    isInitialized_ = false;
  private readonly SessionProvider                                                         sessionProvider_;
  private readonly MongoCollectionProvider<ObjectDataModelMapping, ObjectDataModelMapping> objectCollectionProvider_;
  private readonly Options.ObjectStorage                                                   options_;
  private readonly ILoggerFactory                                                          loggerFactory_;

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);

  public IObjectStorage CreateObjectStorage(string objectStorageName)
  {
    return new ObjectStorage(sessionProvider_, objectCollectionProvider_, loggerFactory_.CreateLogger<ObjectStorage>(),
                             objectStorageName, options_);
  }
}