// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Sockets;

namespace Microsoft.AspNetCore.SignalR.Tests
{
    public class EchoConnectionHandler : ConnectionHandler
    {
        public override async Task OnConnectedAsync(ConnectionContext connection)
        {
            var result = await connection.Transport.Input.ReadAsync();
            var buffer = result.Buffer;

            try
            {
                if (!buffer.IsEmpty)
                {
                    await connection.Transport.Output.WriteAsync(buffer.ToArray());
                }
            }
            finally
            {
                connection.Transport.Input.AdvanceTo(buffer.End);
            }
        }
    }
}
