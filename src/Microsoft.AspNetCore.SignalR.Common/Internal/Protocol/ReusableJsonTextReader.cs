// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using Newtonsoft.Json;

namespace Microsoft.AspNetCore.SignalR.Internal.Protocol
{
    internal class ReusableJsonTextReader : JsonTextReader
    {
        /*
        private readonly TextReader _reader;
        private char[] _chars;
        private int _charsUsed;
        private int _charPos;
        private int _lineStartPos;
        private int _lineNumber;
        private bool _isEndOfFile;
        private StringBuffer _stringBuffer;
        private StringReference _stringReference;
        private IArrayPool<char> _arrayPool;
         */

        private static Action<JsonTextReader, TextReader> _readerField = GetFieldSetter<JsonTextReader, TextReader>("_reader");
        private static Action<JsonTextReader, char[]> _charsField = GetFieldSetter<JsonTextReader, char[]>("_chars");
        private static Func<JsonTextReader, char[]> _getChars = GetFieldGetter<JsonTextReader, char[]>("_chars");
        private static Action<JsonTextReader, int> _charsUsedField = GetFieldSetter<JsonTextReader, int>("_charsUsed");
        private static Action<JsonTextReader, int> _charsPosField = GetFieldSetter<JsonTextReader, int>("_charPos");
        private static Action<JsonTextReader, int> _lineStartPosField = GetFieldSetter<JsonTextReader, int>("_lineStartPos");
        private static Action<JsonTextReader, int> _lineNumberField = GetFieldSetter<JsonTextReader, int>("_lineStartPos");
        private static Action<JsonTextReader, bool> _isEndOfFileField = GetFieldSetter<JsonTextReader, bool>("_isEndOfFile");

        // private static Func<JsonTextReader, int> _stringBufferField = GetFieldInfo("_stringBuffer");
        private static ConstructorInfo _jsonReaderCtor = typeof(JsonReader).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0];

        private Utf8BufferTextReader _reader = new Utf8BufferTextReader();

        public ReusableJsonTextReader() : base(TextReader.Null)
        {
            ArrayPool = JsonArrayPool<char>.Shared;
        }

        public void SetBuffer(ReadOnlyMemory<byte> payload)
        {
            _reader.SetBuffer(payload);

            // HACK: Reset the internal state...
            _readerField(this, _reader);
            _charsField(this, null);
            _charsUsedField(this, 0);
            _charsPosField(this, 0);
            _lineStartPosField(this, 0);
            _lineNumberField(this, 1);
            _isEndOfFileField(this, false);
            // _stringBufferField.SetValue(this, null);
            _jsonReaderCtor.Invoke(this, Array.Empty<object>());
            CloseInput = false;
        }

        public void Reset()
        {
            var chars = _getChars(this);
            ArrayPool.Return(chars);
        }

        public static Func<T, R> GetFieldGetter<T, R>(string fieldName)
        {
            // this => this.fieldName
            ParameterExpression param =
            Expression.Parameter(typeof(T), "this");

            MemberExpression member =
            Expression.Field(param, fieldName);

            LambdaExpression lambda =
            Expression.Lambda(typeof(Func<T, R>), member, param);

            Func<T, R> compiled = (Func<T, R>)lambda.Compile();
            return compiled;
        }

        public static Action<T, A> GetFieldSetter<T, A>(string fieldName)
        {
            // (this, value) => a.fieldName = value
            ParameterExpression param =
            Expression.Parameter(typeof(T), "this");

            ParameterExpression arg =
            Expression.Parameter(typeof(A), "value");

            MemberExpression member =
            Expression.Field(param, fieldName);

            LambdaExpression lambda =
            Expression.Lambda(typeof(Action<T, A>), Expression.Assign(member, arg), arg, param);


            Action<T, A> compiled = (Action<T, A>)lambda.Compile();
            return compiled;
        }


        private static FieldInfo GetFieldInfo(string name)
        {
            // SetProperty(ReusableJsonTextReader)
            return typeof(JsonTextReader).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance);
        }
    }
}