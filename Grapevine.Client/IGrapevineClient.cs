using System;
using System.Reactive.Linq;
using Grapevine.Core;
using ZeroMQ;

namespace Grapevine.Client
{
    /// <summary>
    /// 
    /// </summary>
    public interface IGrapevineClient
    {
        /// <summary>
        /// Publishes a message to the Grapevine server.
        /// </summary>
        void Send<MessageType>(MessageType message, string topic = null);

        /// <summary>
        /// Creates an observable listener for the specified message type. 
        /// No message filtering is performed on the server.
        /// 
        /// NOTE: call Subscribe on the returned observable to receive messages
        /// and dont forget to dispose your subscription when you are done.
        /// 
        /// If you are not using the Rx framework then be sure to add a 
        /// using MaryKay.Grapevine.Extensions to make calling Subscribe
        /// on the observable much easier.
        /// </summary>
        IObservable<MessageType> Receive<MessageType>(string topic = null);

        /// <summary>
        /// Creates an observable listener for the specified message type
        /// and filters messages on the server using the provided filter
        /// expression.
        /// 
        /// NOTE: call Subscribe on the returned observable to receive messages
        /// and dont forget to dispose your subscription when you are done.
        /// 
        /// If you are not using the Rx framework then be sure to add a 
        /// using MaryKay.Grapevine.Extensions to make calling Subscribe
        /// on the observable much easier.
        /// </summary>
        IObservable<MessageType> Receive<MessageType>(Func<MessageType, bool> filter, string topic = null);
    }

    public sealed class GrapevineClient : IGrapevineClient, IDisposable
    {
        IMessageSerializer _serializer = new ProtobufMessageSerializer();
        ZmqContext _context = ZmqContext.Create();
        GrapevineSender _sender;
        GrapevineReceiver _receiver;

        public GrapevineClient(string pubAddress, string subAddress)
        {
            _sender   = new GrapevineSender(_context, subAddress, _serializer);
            _receiver = new GrapevineReceiver(_context, pubAddress, _serializer);
            _receiver.Connect();
        }

        public void Send<MessageType>(MessageType message, string topic = null)
        {
            _sender.Send(message, topic);
        }

        public IObservable<MessageType> Receive<MessageType>(string topic = null)
        {
            MessageTypeRegistry.Register<MessageType>();
            var typeName = MessageTypeRegistry.GetTypeName(typeof(MessageType));
            _receiver.AddTopic(topic ?? typeName);
            return _receiver.OfType<MessageType>();
        }

        public IObservable<MessageType> Receive<MessageType>(Func<MessageType, bool> filter, string topic = null)
        {
            return Receive<MessageType>(topic).Where(filter);
        }

        public void Dispose()
        {
            _sender.Dispose();
            _receiver.Dispose();
 	        _context.Dispose();
        }
    }
}