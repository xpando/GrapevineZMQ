﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;

namespace Grapevine.Core
{
    public class JsonMessageSerializer : IMessageSerializer
    {
        JsonSerializer _serializer;

        public JsonMessageSerializer()
        {
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None
            };

            _serializer = JsonSerializer.Create(settings);
        }

        public byte[] Serialize(object message)
        {
            var ms = new MemoryStream();
            using (var sw = new StreamWriter(ms))
            {
                _serializer.Serialize(sw, message, message.GetType());
                sw.Flush();
                return ms.ToArray();
            }
        }

        public object Deserialize(byte[] buffer, Type type)
        {
            var ms = new MemoryStream(buffer);
            using (var sr = new StreamReader(ms))
            {
                return _serializer.Deserialize(sr, type);
            }
        }
    }
}
