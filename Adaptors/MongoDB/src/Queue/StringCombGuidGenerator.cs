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

using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;

namespace ArmoniK.Core.Adapters.MongoDB.Queue;

public class StringCombGuidGenerator : IIdGenerator
{
  private static CombGuidGenerator Generator
    => CombGuidGenerator.Instance;

  /// <inheritdoc />
  public object GenerateId(object container,
                           object document)
    => $"{Generator.GenerateId(container, document)}";

  /// <inheritdoc />
  public bool IsEmpty(object id)
    => id == null || ((string)id).Equals("00000000-0000-0000-0000-000000000000");

  public static string GenerateId()
    => $"{Generator.GenerateId(null, null)}";
}
