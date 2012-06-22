using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ZeroMQ;
using ZeroMQ.Sockets;

namespace Grapevine.Core
{
    public class GrapevineSender : IDisposable
    {
        IMessageSerializer _serializer;
        IZmqContext _context;
        ISendSocket _socket;

        public GrapevineSender(IZmqContext context, string address, IMessageSerializer serializer)
        {
            _serializer = serializer;
            _context = context;
            _socket = _context.CreatePublishSocket();
            _socket.Connect(address);
        }

        public void Send(object message)
        {
            var typeName = MessageTypeRegistry.GetTypeName(message.GetType());
            var data = _serializer.Serialize(message);

            _socket.SendPart(Encoding.Unicode.GetBytes(typeName));
            _socket.Send(data);
        }

        public void Dispose()
        {
            if (_socket != null)
                _socket.Dispose();

            _socket = null;
        }
    }
}
