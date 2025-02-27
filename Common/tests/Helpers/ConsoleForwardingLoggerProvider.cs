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

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

internal class ConsoleForwardingLoggerProvider : ILoggerProvider
{
  private readonly ForwardingLoggerProvider provider_;

  public ConsoleForwardingLoggerProvider()
  {
    provider_ = new ForwardingLoggerProvider((logLevel,
                                              category,
                                              _,
                                              message,
                                              exception) =>
                                             {
                                               Console.WriteLine(logLevel + " => " + category + "\n" + message + "\n" + exception);
                                             });
  }

  public void Dispose()
    => provider_.Dispose();

  public ILogger CreateLogger(string categoryName)
    => provider_.CreateLogger(categoryName);
}
