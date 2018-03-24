// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.Extensions.ObjectPool;

namespace Microsoft.AspNetCore.SignalR.Internal.Protocol
{
    internal static class SharedObjectPool<T> where T : class, new()
    {
        private static readonly DefaultObjectPool<T> Instance = new DefaultObjectPool<T>(new DefaultPooledObjectPolicy<T>());

        public static PooledScope<T> Get() => new PooledScope<T>(Instance);
    }

    internal readonly struct PooledScope<T> : IDisposable where T : class
    {
        private readonly DefaultObjectPool<T> _pool;
        private readonly T _item;
        public T Item => _item;
        public PooledScope(DefaultObjectPool<T> pool)
        {
            _pool = pool;
            _item = pool.Get();
        }
        public void Dispose()
        {
            _pool.Return(_item);
        }
    }
}