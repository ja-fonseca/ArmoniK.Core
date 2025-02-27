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

using JetBrains.Annotations;

namespace ArmoniK.Samples.HtcMock.Client.Options;

[PublicAPI]
public class HtcMock
{
  public const string   SettingSection = nameof(HtcMock);
  public       int      NTasks               { get; set; } = 100;
  public       TimeSpan TotalCalculationTime { get; set; } = TimeSpan.FromMilliseconds(100);
  public       int      DataSize             { get; set; } = 1;
  public       int      MemorySize           { get; set; } = 1;
  public       int      SubTasksLevels       { get; set; } = 4;
  public       bool     FastCompute          { get; set; } = true;
  public       bool     UseLowMem            { get; set; } = true;
  public       bool     SmallOutput          { get; set; } = true;
}
