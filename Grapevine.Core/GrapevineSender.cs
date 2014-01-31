using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ZeroMQ;

namespace Grapevine.Core
{
    public class GrapevineSender : IDisposable
    {
        IMessageSerializer  _serializer;
        EventLoopScheduler  _scheduler;
        ZmqSocket           _socket;
        Subject<ZmqMessage> _messages;
        IDisposable         _messageDispatcher;

        public GrapevineSender(ZmqContext context, string address, IMessageSerializer serializer)
        {
            _serializer = serializer;
            _scheduler = new EventLoopScheduler();

            _socket = context.CreateSocket(SocketType.PUB);
            _socket.Connect(address);

            _messages = new Subject<ZmqMessage>();
            _messageDispatcher = _messages
                .SubscribeOn(_scheduler)
                .ObserveOn(_scheduler)
                .Subscribe(msg => _socket.SendMessage(msg));
        }

        public void Send(object message, string topic = null)
        {
            var typeName = MessageTypeRegistry.GetTypeName(message.GetType());
            var data = _serializer.Serialize(message);

            if (topic == null)
                topic = typeName;

            var msg = new ZmqMessage();
            msg.Append(new Frame(ZmqContext.DefaultEncoding.GetBytes(topic)));
            msg.Append(new Frame(ZmqContext.DefaultEncoding.GetBytes(typeName)));
            msg.Append(new Frame(data));

            _messages.OnNext(msg);
        }

        public void Dispose()
        {
            _messageDispatcher.Dispose();
            _messages.Dispose();
            _scheduler.Dispose();
            _socket.Dispose();
        }
    }
}
