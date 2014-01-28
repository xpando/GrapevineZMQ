using System.Configuration;
using NLog;
using Topshelf;
using ZeroMQ;
using ZeroMQ.Devices;

namespace Grapevine.Server
{
    class ForwarderService
    {
        static Logger _logger = LogManager.GetCurrentClassLogger();

        ZmqContext _context;
        ForwarderDevice _forwarder;

        public ForwarderService(string frontend, string backend)
        {
            _logger.Info("Creating 0mq context.");
            _context = ZmqContext.Create();
            _logger.Info("done.");

            _logger.Info("Creating 0mq forwarder device.");
            _forwarder = new ForwarderDevice(_context, frontend, backend, DeviceMode.Threaded);
            _logger.Info("done.");

            _logger.Info("Subscribe all on frontend.");
            _forwarder.FrontendSetup.SubscribeAll();
            _logger.Info("done.");
        }

        public void Start()
        {
            _logger.Info("Starting forwarder device.");
            _forwarder.Start();
            _logger.Info("done.");
        }

        public void Pause()
        {
            _logger.Info("Stopping forwarder device.");
            _forwarder.Stop();
            _logger.Info("done.");
        }

        public void Stop()
        {
            _logger.Info("Disposing forwarder device.");
            _forwarder.Dispose();
            _logger.Info("done.");

            _logger.Info("Disposing 0mq context.");
            _context.Dispose();
            _logger.Info("done.");
        }
    }

    class Program
    {
        static string Frontend
        {
            get
            {
                var frontend = ConfigurationManager.AppSettings["frontend"];

                if (string.IsNullOrEmpty(frontend))
                    throw new ConfigurationErrorsException("Missing frontend appSetting");

                return frontend;
            }
        }

        static string Backend
        {
            get
            {
                var backend = ConfigurationManager.AppSettings["backend"];

                if (string.IsNullOrEmpty(backend))
                    throw new ConfigurationErrorsException("Missing backend appSetting");

                return backend;
            }
        }

        static void Main(string[] args)
        {
            HostFactory.Run(x =>
            {
                x.Service<ForwarderService>(s =>
                {
                    s.ConstructUsing(name => new ForwarderService(Frontend,Backend));
                    s.WhenStarted(svc => svc.Start());
                    s.WhenPaused(svc => svc.Pause());
                    s.WhenContinued(svc => svc.Start());
                    s.WhenStopped(svc => svc.Stop());
                });

                x.StartAutomaticallyDelayed();
                x.RunAsNetworkService();
                x.SetDescription("Grapevine Message Forwarding Service");
                x.SetDisplayName("GrapevineForwarder");
                x.SetServiceName("GrapevineForwarder");
            });
        }
    }
}
