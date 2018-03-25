// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.IO;
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

        private static FieldInfo _readerField = GetFieldInfo("_reader");
        private static FieldInfo _charsField = GetFieldInfo("_chars");
        private static FieldInfo _charsUsedField = GetFieldInfo("_charsUsed");
        private static FieldInfo _charsPosField = GetFieldInfo("_charPos");
        private static FieldInfo _lineStartPosField = GetFieldInfo("_lineStartPos");
        private static FieldInfo _lineNumberField = GetFieldInfo("_lineStartPos");
        private static FieldInfo _isEndOfFileField = GetFieldInfo("_isEndOfFile");
        private static FieldInfo _stringBufferField = GetFieldInfo("_stringBuffer");
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
            _readerField.SetValue(this, _reader);
            _charsField.SetValue(this, null);
            _charsUsedField.SetValue(this, 0);
            _charsPosField.SetValue(this, 0);
            _lineStartPosField.SetValue(this, 0);
            _lineNumberField.SetValue(this, 1);
            _isEndOfFileField.SetValue(this, false);
            _stringBufferField.SetValue(this, null);
            _jsonReaderCtor.Invoke(this, Array.Empty<object>());
            CloseInput = false;
        }

        public void Reset()
        {
            var chars = (char[])_charsField.GetValue(this);
            ArrayPool.Return(chars);
        }

        private static FieldInfo GetFieldInfo(string name)
        {
            return typeof(JsonTextReader).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance);
        }
    }
}